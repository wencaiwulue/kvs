package raft;

import db.core.Config;
import db.core.DB;
import db.core.LogDB;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.enums.Role;
import raft.processor.Processor;
import rpc.RpcClient;
import rpc.model.requestresponse.*;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
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
    public static final Logger LOGGER = LogManager.getLogger(Node.class);

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

    public volatile long nextElectTime = this.nextElectTime();// 下次选举时间，总是会因为心跳而推迟，会在因为主leader down后开始选举
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

    private List<Processor> processors = new ArrayList<>(10);

    private Runnable heartbeatTask;
    public Runnable electTask;

    public Node(NodeAddress address, Set<NodeAddress> allNodeAddresses) {
        this.address = address;
        this.allNodeAddresses = allNodeAddresses;
        this.db = new DB(Config.DB_DIR);
        this.logdb = new LogDB(Config.LOG_DIR);
        this.nextIndex = this.logdb.lastLogIndex + 1;
    }

    // 放在代码块中，每次创建对象的时候会自动调用
    {
        // 这里使用SPI，可以通过配置文件修改实现
        ServiceLoader.load(Processor.class).iterator().forEachRemaining(this.processors::add);
    }

    @Override
    public void run() {
        this.electTask = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextElectTime > System.nanoTime()) {
                return;
            }

            // electing start
            elect();
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.electTask, 0, 10, TimeUnit.MILLISECONDS);

        this.heartbeatTask = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextHeartbeatTime > System.nanoTime()) {
                return;
            }

            if (isLeader()) {// 如果自己是主领导，就需要给各个节点发送心跳包
                for (NodeAddress remote : this.allNodeAddressExcludeMe()) {
//                    if (!remote.alive) {
//                        LOGGER.error("remote node is down");
//                        continue;
//                    }
                    Response response = RpcClient.doRequest(remote, new AppendEntriesRequest(Collections.emptyList(), this.address, this.currentTerm, this.lastAppliedTerm, this.lastAppliedIndex.intValue(), this.committedIndex));

                    // install snapshot
                    if (response instanceof ErrorResponse) {// todo 这里约定的是如果心跳包回复ErrorResponse, 说明是out的节点，需要安装更新
                        long size = 0;
                        try {
                            // todo 这里需要根据index确定是哪一个文件，并且index要做到全局递增，这个怎么办？
                            size = FileChannel.open(this.logdb.file.get(0).toPath(), StandardOpenOption.READ).size();
                        } catch (ClosedChannelException e) {
                            LOGGER.error("who close the channel !!");
                        } catch (IOException e) {
                            LOGGER.error(e);
                        }
                        InstallSnapshotResponse snapshotResponse = (InstallSnapshotResponse) RpcClient.doRequest(remote, new InstallSnapshotRequest(this.address, this.currentTerm, this.logdb.dir.toString(), size));
                        if (snapshotResponse == null || !snapshotResponse.isSuccess()) {
                            LOGGER.error("Install snapshot error, should retry?");
                        }
                    }

                    LOGGER.error("收到follower:{}的心跳回包", remote.getSocketAddress().getPort());
                }
                this.nextElectTime = this.nextElectTime();
                this.nextHeartbeatTime = System.nanoTime() + Config.HEARTBEAT_RATE.toNanos();
                LOGGER.error("成功推迟选举");
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.heartbeatTask, 0, 10, TimeUnit.MILLISECONDS);
        this.start = true;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void elect() {
        if (this.allNodeAddressExcludeMe().isEmpty()) { // only one node
            LOGGER.error("singleton cluster");
            return;
        }

        LOGGER.error("start elect...");
        long start = System.nanoTime();
        this.writeLock.lock();
        try {
            AtomicInteger ai = new AtomicInteger(1);//先给自己投一票
            AtomicBoolean fail = new AtomicBoolean(false);
            this.role = Role.CANDIDATE;// 改变状态为candidate
            CountDownLatch latch = new CountDownLatch(this.allNodeAddressExcludeMe().size());
            Future<?>[] futures = new Future[this.allNodeAddressExcludeMe().size()];
            int p = 0;
            for (NodeAddress addr : this.allNodeAddressExcludeMe()) {
                Runnable r = () -> {
                    try {
                        VoteResponse response = (VoteResponse) RpcClient.doRequest(addr, new VoteRequest(this.address, this.currentTerm + 1, this.logdb.lastLogIndex, this.logdb.lastLogTerm));
                        if (response != null) {
                            LOGGER.error("收到从:{}的投票回包:{}", addr, response);
                            if (response.isGrant()) {
                                ai.addAndGet(1);
                            } else if (response.getTerm() > this.currentTerm) {
                                this.role = Role.FOLLOWER;
                                this.currentTerm = response.getTerm();
                                fail.set(true);
                            } else {
                                LOGGER.error("竟然不投票。远端主机为: {}", addr);
                            }
                        } else {
                            LOGGER.error("报错了。远端主机为: {}", addr);
                        }
                    } finally {
                        latch.countDown();
                    }
                };
                futures[p++] = ThreadUtil.getThreadPool().submit(r);
            }
            try {
                boolean a = latch.await(Config.ELECT_RATE.toMillis(), TimeUnit.MILLISECONDS);
                if (!a) {
                    Arrays.stream(futures).forEach(e -> e.cancel(true));
                    LOGGER.error("elect timeout, cancel all task");
                    return;
                }
            } catch (InterruptedException exception) {
                Arrays.stream(futures).forEach(e -> e.cancel(true));
                LOGGER.error("elect interrupted, cancel all task");
            }
            if (fail.getAcquire()) {
                return;
            }
            // 0-150ms, 随机一段时间，避免同时选举
            int i = BigDecimal.valueOf(this.allNodeAddresses.size() / 2D).setScale(0, RoundingMode.UP).intValue();
            if (ai.get() >= i) {// 超过半数了，成功了
                LOGGER.error("选举成功，选出leader了{}", this.address);
                this.currentTerm = this.currentTerm + 1;
                this.leaderAddress = this.address;
                this.role = Role.LEADER;
                this.nextHeartbeatTime = -1;// 立即心跳
                ThreadUtil.getThreadPool().submit(this.heartbeatTask);
            } else {
                LOGGER.error("ticket: {} and expect: {}", ai.get(), i);
                LOGGER.error("elect failed");
            }
            this.nextElectTime = this.nextElectTime();// 0-150ms, 随机一段时间，避免同时选举
        } finally {
            System.out.println("选举花费时间：" + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " ms");
            this.writeLock.unlock();
        }
    }


    public static boolean isDead(SocketChannel channel) {
        return channel == null || !channel.isOpen() || !channel.isConnected();
    }

    public boolean isLeader() {
        return leaderAddress != null && leaderAddress.equals(this.address) && role == Role.LEADER;
    }

    // 可以拆分为服务器之间与服务器和client两种，也就是状态协商和curd
    public void handle(Request request, SocketChannel channel) {
        if (request == null) return;
        if (!this.start) return;

        Response response = null;
        for (Processor processor : this.processors) {
            if (processor.supports(request)) {
                response = processor.process(request, this);
                break;
            }
        }

        if (response != null) {
            response.requestId = request.requestId;
        }

        try {
            channel.write(FSTUtil.asArrayWithLength(response));
        } catch (ClosedChannelException e) {
            LOGGER.error("这里的channel失效了, 需要重试吗?", e);
        } catch (IOException e) {
            LOGGER.error("回复失败。", e);
        }
    }

    public Set<NodeAddress> allNodeAddressExcludeMe() {
        HashSet<NodeAddress> nodeAddresses = new HashSet<>(this.allNodeAddresses);
        nodeAddresses.remove(this.address);
        return nodeAddresses;
    }

    public long nextElectTime() {
        return System.nanoTime() + Config.ELECT_RATE.toNanos() + TimeUnit.MILLISECONDS.toNanos(ThreadLocalRandom.current().nextInt(100));// randomize 0--100 ms
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
