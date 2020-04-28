package raft;

import db.core.DB;
import db.core.LogDB;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.enums.Role;
import raft.processor.*;
import rpc.Client;
import rpc.model.requestresponse.*;
import util.ByteArrayUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public static final Logger log = LogManager.getLogger(Node.class);

    public volatile boolean start;

    public NodeAddress address; // 本机的IP和端口信息
    public Set<NodeAddress> allNodeAddresses; //所有节点的IP和端口信息, 包含当前节点

    public DB db;
    public LogDB logdb;
    public Role role = Role.FOLLOWER;// 默认是follower角色

    public volatile long committedIndex;
    private AtomicLong lastAppliedIndex = new AtomicLong(0);
    private volatile int lastAppliedTerm = 0;
    public volatile long nextIndex;

    final long heartBeatRate = 20;// the last and this heart beat difference is 150ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    final long electRate = 400;// the last and this heart beat difference is 150ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    public volatile long nextElectTime = this.delayElectTime();// 下次选举时间，总是会因为心跳而推迟，会在因为主leader down后开始选举
    public volatile long nextHeartbeatTime /*= System.nanoTime() + this.heartBeatRate*/;// 下次心跳时间。对leader有用
    //150ms -- 300ms randomized  超时时间,选举的时候，如果主节点挂了，则所有的节点开始timeout，然后最先timeout结束的节点变为candidate，
    // 参见竞选，然后发送竞选类型的请求，如果半数以上统一，则广播给所有人，
    // leader回一直发送心跳包，如果timeout后还没有发现心跳包来，就说明leader挂了，需要开始选举

    public volatile int currentTerm = 0;// 第几任leader
    public volatile NodeAddress lastVoteFor;// 判断当前选举是否已经投票
    public volatile NodeAddress leaderAddress;//leader节点信息，因为所有的数据处理都需要leader来操作。

    public ReadWriteLock lock = new ReentrantReadWriteLock();
    public Lock readLock = this.lock.readLock();
    public Lock writeLock = this.lock.writeLock();

    private List<Processor> processors;
    private List<Processor> KVProcessors;

    private Runnable heartbeat;
    public Runnable elect;

    public Node(NodeAddress address, Set<NodeAddress> allNodeAddresses) {
        this.address = address;
        this.allNodeAddresses = allNodeAddresses;
        this.db = new DB("C:\\Users\\89570\\Documents\\kvs_" + address.getSocketAddress().getPort() + "_db");
        this.logdb = new LogDB("C:\\Users\\89570\\Documents\\kvs_" + address.getSocketAddress().getPort() + ".log");
        this.nextIndex = this.logdb.lastLogIndex + 1;
        this.processors = Arrays.asList(new AddPeerRequestProcessor(), new RemovePeerRequestProcessor(), new VoteRequestProcessor(), new PowerRequestProcessor(), new DownloadFileRequestProcessor());
        this.KVProcessors = Arrays.asList(new AppendEntriesRequestProcessor(), new InstallSnapshotRequestProcessor(), new CURDProcessor());
    }

    @Override
    public void run() {
        this.elect = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextElectTime > System.nanoTime()) {
//                System.out.println("还剩：" + TimeUnit.NANOSECONDS.toMillis(this.nextElectTime - System.nanoTime()) + "ms开始选举");
                return; // sleep until it's time to electing
            }

            // electing start
            elect();
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(elect, 0, 10, TimeUnit.MILLISECONDS);

        this.heartbeat = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextHeartbeatTime > System.nanoTime()) {
                return; // sleep until it's time to heartbeat
            }

            if (isLeader()) {// 如果自己是主领导，就需要给各个节点发送心跳包
                for (NodeAddress remote : this.allNodeAddressExcludeMe()) {
                    if (!remote.alive) continue;
                    Response response = Client.doRequest(remote, new AppendEntriesRequest(Collections.emptyList(), this.address, this.currentTerm, this.lastAppliedTerm, this.lastAppliedIndex.intValue(), this.committedIndex));

                    // install snapshot
                    if (response instanceof ErrorResponse) {
                        long size = 0;
                        try {
                            size = FileChannel.open(this.logdb.file.get(0).toPath(), StandardOpenOption.READ).size();
                        } catch (ClosedChannelException e) {
                            log.error("who close the channel !!");
                        } catch (IOException e) {
                            log.error(e);
                        }
                        InstallSnapshotResponse snapshotResponse = (InstallSnapshotResponse) Client.doRequest(remote, new InstallSnapshotRequest(this.address, this.currentTerm, this.logdb.logDBPath.toString(), size));
                        if (snapshotResponse == null || !snapshotResponse.isSuccess()) {
                            log.error("Install snapshot error, should retry?");
                        }
                    }

                    log.error("收到follower:{}的心跳回包", remote.getSocketAddress().getPort());
                }
                this.nextElectTime = this.delayElectTime();
                this.nextHeartbeatTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.heartBeatRate);
//                System.out.println("成功给推迟选举");
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.heartbeat, 0, 10, TimeUnit.MILLISECONDS);
        this.start = true;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void elect() {
        if (this.allNodeAddressExcludeMe().isEmpty()) { // only one node
            return;
        }

        log.error("start elect...");
        long start = System.nanoTime();
        this.writeLock.lock();
        try {
            AtomicInteger ai = new AtomicInteger(1);//先给自己投一票
            AtomicBoolean fail = new AtomicBoolean(false);
            this.role = Role.CANDIDATE;// 改变状态为candidate
            CountDownLatch latch = new CountDownLatch(this.allNodeAddressExcludeMe().size());
            for (NodeAddress addr : this.allNodeAddressExcludeMe()) {
                Runnable r = () -> {
                    try {
                        VoteResponse response = (VoteResponse) Client.doRequest(addr, new VoteRequest(this.address, this.currentTerm + 1, this.logdb.lastLogIndex, this.logdb.lastLogTerm));
                        if (response != null) {
                            log.error("收到从:{}的投票回包:{}", addr, response);
                            if (response.isGrant()) {
                                ai.addAndGet(1);
                            } else if (response.getTerm() > this.currentTerm) {
                                this.role = Role.FOLLOWER;
                                this.currentTerm = response.getTerm();
                                fail.set(true);
                            } else {
                                log.error("竟然不投票。远端主机为: {}", addr);
                            }
                        } else {
                            log.error("可能是挂掉了。远端主机为: {}", addr);
                        }
                    } finally {
                        latch.countDown();
                    }
                };
                ThreadUtil.getThreadPool().execute(r);
            }
            latch.await(this.electRate, TimeUnit.MILLISECONDS);
            if (fail.getAcquire()) {
                return;
            }
            // 0-150ms, 随机一段时间，避免同时选举
            if (ai.get() > Math.ceil(this.allNodeAddresses.size() / 2D)) {// 超过半数了，成功了
                log.error("选举成功，选出leader了{}", this.address);
                this.currentTerm = this.currentTerm + 1;
                this.leaderAddress = this.address;
                this.role = Role.LEADER;
                this.nextHeartbeatTime = -1;// 立即心跳
                ThreadUtil.getThreadPool().execute(this.heartbeat);
            } else {
                log.error("elect failed");
            }
            this.nextElectTime = this.delayElectTime();// 0-150ms, 随机一段时间，避免同时选举
        } catch (InterruptedException e) {
            log.error(e);
        } finally {
            System.out.println("选举花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
            this.writeLock.unlock();
        }
    }


    public static boolean checkAlive(SocketChannel channel) {
        return channel != null && channel.isOpen() && channel.isConnected();
    }

    public boolean isLeader() {
        return leaderAddress != null && leaderAddress.equals(this.address) && role == Role.LEADER;
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
            channel.write(ByteArrayUtil.write(r));
        } catch (ClosedChannelException e) {
            log.error("这里的channel失效了, 需要重试吗?");
        } catch (IOException e) {
            log.error("回复失败。", e);
        }
    }

    public Set<NodeAddress> allNodeAddressExcludeMe() {
        HashSet<NodeAddress> nodeAddresses = new HashSet<>(this.allNodeAddresses);
        nodeAddresses.remove(this.address);
        return nodeAddresses;
    }

    public long delayElectTime() {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(this.electRate + ThreadLocalRandom.current().nextInt(100));
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
