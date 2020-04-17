package raft;

import db.core.DB;
import db.core.LogDB;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.enums.State;
import raft.processor.*;
import rpc.Client;
import rpc.model.requestresponse.*;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author naison
 * @since 3/14/2020 19:05
 */
@Data
public class Node implements Runnable {
    private static final Logger log = LogManager.getLogger(Node.class);

    public volatile boolean start;

    public InetSocketAddress address; // 本机的IP和端口信息
    public Set<InetSocketAddress> peerAddress; //其它节点的IP和端口信息, 不包含当前节点

    public DB db;
    public LogDB logdb;
    public State state = State.FOLLOWER;// 默认是follower角色
    long timeout = 1000;//150ms -- 300ms randomized  超时时间,选举的时候，如果主节点挂了，则所有的节点开始timeout，然后最先timeout结束的节点变为candidate，
    // 参见竞选，然后发送竞选类型的请求，如果半数以上统一，则广播给所有人，
    // leader回一直发送心跳包，如果timeout后还没有发现心跳包来，就说明leader挂了，需要开始选举

    AtomicLong l = new AtomicLong(0);// 用来生成提案编号使用

    public volatile long lastHeartBeat;
    final long heartBeatRate = 1000;// the last and this heart beat difference is 150ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader

    public volatile int currTerm = 0;// 第几任leader
    public volatile InetSocketAddress lastVoteFor;// 判断当前选举是否已经投票
    public volatile InetSocketAddress leaderAddr;//leader节点信息，因为所有的数据处理都需要leader来操作。

    public ReadWriteLock lock = new ReentrantReadWriteLock();
    public Lock readLock = this.lock.readLock();
    public Lock writeLock = this.lock.writeLock();

    private List<Processor> processors;
    private List<Processor> KVProcessors;

    public Node(InetSocketAddress address, Set<InetSocketAddress> peerAddress) {
        this.address = address;
        this.peerAddress = peerAddress;
        this.db = new DB("C:\\Users\\89570\\Documents\\kvs_" + address.getPort() + ".db");
        this.logdb = new LogDB("C:\\Users\\89570\\Documents\\kvs_" + address.getPort() + ".log");
        this.processors = Arrays.asList(new AddPeerRequestProcessor(), new HeartbeatRequestProcessor(), new RemovePeerRequestProcessor(), new VoteRequestProcessor(), new PowerRequestProcessor());
        this.KVProcessors = Arrays.asList(new AppendEntriesRequestProcessor(), new ReplicationRequestProcessor(), new CURDProcessor());
    }

    @Override
    public void run() {
        Runnable elect = () -> {
            if (!start) return;

            int i = ThreadLocalRandom.current().nextInt(150);// 0-150ms, 随机一段时间，避免同时选举
            if (leaderAddr == null || (System.nanoTime() > lastHeartBeat + heartBeatRate + i && !this.isLeader())) {// 很久没有来自leader的心跳，说明leader卒了，选举开始
                elect();
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(elect, 100, heartBeatRate, TimeUnit.MILLISECONDS);// 定期检查leader是不是死掉了

        Runnable heartbeat = () -> {
            if (!start) return;

            if (isLeader()) {// 如果自己是主领导，就需要给各个节点发送心跳包
                for (InetSocketAddress address : peerAddress) {
                    if (address.equals(this.address)) continue;// 自己不发

                    HeartbeatResponse response = (HeartbeatResponse) Client.doRequest(address, new HeartbeatRequest(this.currTerm, this.address));
                    log.error("收到从follower:{}的心跳回包:{}", address.getPort(), response);
                }
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(heartbeat, 0, heartBeatRate, TimeUnit.MILLISECONDS);// 每150ms心跳一下
        start = true;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void elect() {
        if (this.peerAddress.isEmpty()) { // only one node, became the leader
            log.error("i'm a standalone leader");
            this.currTerm = this.currTerm + 1;
            this.leaderAddr = this.address;
            this.state = State.LEADER;
            return;
        }

        log.error("start elect...");
        this.writeLock.lock();
        try {
            AtomicInteger ai = new AtomicInteger(1);//先给自己投一票
            this.state = State.CANDIDATE;// 改变状态为candidate
            CountDownLatch latch = new CountDownLatch(this.peerAddress.size());
            for (InetSocketAddress addr : this.peerAddress) {
                Runnable r = () -> {
                    VoteResponse response = (VoteResponse) Client.doRequest(addr, new VoteRequest(this.address, this.currTerm + 1, this.logdb.lastLogIndex, this.logdb.lastLogTerm));
                    if (response != null) {
                        log.error("收到从:{}的回包:{}", addr, response);
                        if (response.isGrant()) {
                            ai.addAndGet(1);
                        } else {
                            log.error("竟然不投票。远端主机为: {}", addr);
                        }
                    } else {
                        log.error("可能是挂掉了。远端主机为: {}", addr);
                    }
                    latch.countDown();
                };
                ThreadUtil.getThreadPool().execute(r);
            }
            latch.await(1, TimeUnit.MINUTES);
            if (ai.get() > Math.ceil(peerAddress.size() / 2D)) {// 超过半数了，成功了
                log.error("选举成功，选出leader了{}", this.address);
                this.currTerm = this.currTerm + 1;
                this.leaderAddr = this.address;
                this.state = State.LEADER;
            } else {
                log.error("elect failed");
            }
        } catch (InterruptedException e) {
            log.error(e);
        } finally {
            this.writeLock.unlock();
        }
    }


    public static boolean checkAlive(SocketChannel channel) {
        return channel != null && channel.isOpen() && channel.isConnected();
    }

    public boolean isLeader() {
        return leaderAddr != null && leaderAddr.equals(this.address) && state == State.LEADER;
    }

    // 可以拆分为服务器之间与服务器和client两种，也就是状态协商和curd
    public void handle(Request req, SocketChannel channel) {
        if (req == null) return;
        if (!this.start) return;

        Response r = null;
        List<Processor> processorList = new ArrayList<>(this.processors);
        processorList.addAll(this.KVProcessors);

        for (Processor processor : processorList) {
            if (processor.supports(req)) {
                r = processor.process(req, this);
                break;
            }
        }

        if (r != null) {
            r.requestId = req.requestId;
        }

        try {
            channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(r)));
        } catch (ClosedChannelException e) {
            log.error("这里的channel失效了, 需要重试吗?");
        } catch (IOException e) {
            log.error("回复失败。", e);
        }
    }

    /*
     *
     *  Leader Election.主节点选举
     * if(follower没有听见来自leader的心跳){
     *     1,那么回自己生成一个随机的timeout事件
     *     2,开始倒计时
     *     if(倒计时结束还没有leader出来){
     *         1，自己就变成candidate。
     *         2，然后发送选举类型的请求包
     *         3，同步等待100毫秒
     *         if(timeout，则重试前判断是否有leader出来){
     *
     *         }else{
     *              1,是否超过半数，
     *              if(超过N/2){
     *                  则为leader, 这里迅速发送心跳包，并且通知各位修改leader节点信息
     *              }else{
     *                  判断是否有leader出来
     *                  if(没有){
     *                      重试
     *                  }else{
     *                      break;
     *                  }
     *              }
     *
     *         }
     *     }
     * }
     */

    /*
     * 更改数据
     * 1，从client接受请求，判断当前节点，是不是主节点
     * if(如果不是主节点 && 主节点存在){
     *     转发请求到主节点
     * }else if(如果不是主节点 && 主节点不存在){
     *      1，主节点选举。in
     *      2，转发到新选举出来的主节点
     * }else if(是主节点 && 主节点不存在){
     *      1，说明主节点已失效。
     *      2，清除主节点信息
     *      3，主节点选取
     *      4，转发到主节点
     * }else if(是主节点 && 主节点存在){
     *      1，写入log，但是不是提交状态
     *      2，rpc请求给所有的非follower节点，写入log, 但是不是提交状态
     *      3，rpc回包是否超过半数ok
     *      if(超过半数以上回包ok){
     *          1, 主节点提交写入日志，也就是undo log 或者是redo log
     *          2, 第二次rpc说是同志们都提交吧，然后所有的节点都commit
     *      }else(没有达到半数){
     *          1，该请求失败，抛出异常
     *      }
     * }
     *
     *
     */

    /*
     * 子问题，如何确定是不是有有效的leader存在
     *
     */

}
