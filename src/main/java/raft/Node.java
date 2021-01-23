package raft;

import com.google.common.collect.Sets;
import db.core.Config;
import db.core.DB;
import db.core.LogDB;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.enums.Role;
import raft.processor.Processor;
import rpc.model.requestresponse.*;
import rpc.netty.pub.RpcClient;
import rpc.netty.server.WebSocketServer;
import util.ThreadUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    public volatile boolean start;

    // current node address
    public NodeAddress address;
    // all nodes address, include current node
    public Set<NodeAddress> allNodeAddresses;

    public DB db;
    public LogDB logdb;
    // default role is follower
    public Role role = Role.FOLLOWER;

    public volatile long committedIndex;
    private AtomicLong lastAppliedIndex = new AtomicLong(0);
    private volatile int lastAppliedTerm = 0;
    public volatile long nextIndex;

    // first elect can be longer than normal election time
    public volatile long nextElectTime = this.nextElectTime() + TimeUnit.SECONDS.toMillis(5);
    public volatile long nextHeartbeatTime /*= System.currentTimeMillis() + this.heartBeatRate*/;

    public volatile int currentTerm = 0;
    // current term whether voted or not
    public volatile NodeAddress lastVoteFor;
    // cluster leader address
    public volatile NodeAddress leaderAddress;

    public ReadWriteLock lock = new ReentrantReadWriteLock();
    public Lock readLock = this.lock.readLock();
    public Lock writeLock = this.lock.writeLock();

    private List<Processor> processors = new ArrayList<>(10);

    private Runnable heartbeatTask;
    public Runnable electTask;

    public static Node of(NodeAddress nodeAddress) {
        InetSocketAddress p8001 = new InetSocketAddress("127.0.0.1", 8001);
        InetSocketAddress p8002 = new InetSocketAddress("127.0.0.1", 8002);
        InetSocketAddress p8003 = new InetSocketAddress("127.0.0.1", 8003);
        NodeAddress p1 = new NodeAddress(p8001);
        NodeAddress p2 = new NodeAddress(p8002);
        NodeAddress p3 = new NodeAddress(p8003);
        return new Node(nodeAddress, Sets.newHashSet(p1, p2, p3));
    }

    public Node(NodeAddress address, Set<NodeAddress> allNodeAddresses) {
        this.address = address;
        this.allNodeAddresses = allNodeAddresses;
        this.db = new DB(Config.DB_DIR);
        this.logdb = new LogDB(Config.LOG_DIR);
        this.nextIndex = this.logdb.lastLogIndex + 1;
    }

    {
        // use SPI to load business processors
        ServiceLoader.load(Processor.class).iterator().forEachRemaining(this.processors::add);
    }

    @Override
    public void run() {
        this.electTask = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextElectTime > System.currentTimeMillis()) {
                return;
            }

            this.role = Role.CANDIDATE;

            this.elect();
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.electTask, 0, Config.ELECT_RATE.toMillis(), TimeUnit.MILLISECONDS);

        this.heartbeatTask = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextHeartbeatTime > System.currentTimeMillis()) {
                return;
            }

            // if current node role is leader, then needs to send heartbeat to other, tell them i am boss, i rule anything !!!
            if (isLeader()) {
                for (NodeAddress remote : this.allNodeAddressExcludeMe()) {
                    Response response = RpcClient.doRequest(remote, new AppendEntriesRequest(Collections.emptyList(), this.address, this.currentTerm, this.lastAppliedTerm, this.lastAppliedIndex.intValue(), this.committedIndex));
                    // install snapshot
                    if (response instanceof ErrorResponse) {// todo 这里约定的是如果心跳包回复ErrorResponse, 说明是out的节点，需要安装更新
                        long size = 0;
                        try {
                            // todo 这里需要根据index确定是哪一个文件，并且index要做到全局递增，这个怎么办？
                            size = FileChannel.open(this.logdb.file.get(0).toPath(), StandardOpenOption.READ).size();
                        } catch (ClosedChannelException e) {
                            LOGGER.error("who close the channel ?!!!");
                        } catch (IOException e) {
                            LOGGER.error(e.getMessage());
                        }
                        InstallSnapshotResponse snapshotResponse = (InstallSnapshotResponse) RpcClient.doRequest(remote, new InstallSnapshotRequest(this.address, this.currentTerm, this.logdb.dir.toString(), size));
                        if (snapshotResponse == null || !snapshotResponse.isSuccess()) {
                            LOGGER.error("Install snapshot error, should retry?");
                        }
                    }

                    LOGGER.error("{} --> {}, receive heartbeat response", remote.getSocketAddress().getPort(), WebSocketServer.PORT);
                }
                this.nextElectTime = this.nextElectTime();
                this.nextHeartbeatTime = System.currentTimeMillis() + Config.HEARTBEAT_RATE.toMillis();
                LOGGER.info("Delay elect successfully");
            }
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.heartbeatTask, 0, Config.HEARTBEAT_RATE.toMillis(), TimeUnit.MILLISECONDS);
        this.start = true;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void elect() {
        // only one node
        if (this.allNodeAddressExcludeMe().isEmpty()) {
            LOGGER.info("Singleton cluster");
            return;
        }

        LOGGER.info("Start elect...");
        long start = System.nanoTime();
        this.writeLock.lock();
        try {
            this.currentTerm++;
            AtomicInteger ticket = new AtomicInteger(1);//先给自己投一票
            AtomicBoolean fail = new AtomicBoolean(false);
            CountDownLatch latch = new CountDownLatch(this.allNodeAddressExcludeMe().size());
            Future<?>[] futures = new Future[this.allNodeAddressExcludeMe().size()];
            int index = 0;
            for (NodeAddress addr : this.allNodeAddressExcludeMe()) {
                Runnable r = () -> {
                    try {
                        VoteResponse response = (VoteResponse) RpcClient.doRequest(addr, new VoteRequest(this.address, this.currentTerm, this.logdb.lastLogIndex, this.logdb.lastLogTerm));
                        if (response != null) {
                            LOGGER.error("{} --> {}, vote response: {}", addr.getSocketAddress().getPort(), WebSocketServer.PORT, response.toString());
                            if (this.role.equals(Role.CANDIDATE) && response.isGrant() && response.getTerm() == this.currentTerm) {
                                ticket.incrementAndGet();
                            } else if (response.getTerm() > this.currentTerm) {
                                this.role = Role.FOLLOWER;
                                this.currentTerm = response.getTerm();
                                fail.set(true);
                            } else {
                                LOGGER.error("{} --> {}, no vote response", addr.getSocketAddress().getPort(), WebSocketServer.PORT);
                            }
                        } else {
                            LOGGER.error("{} --> {}, null vote response", addr.getSocketAddress().getPort(), WebSocketServer.PORT);
                        }
                    } finally {
                        latch.countDown();
                    }
                };
                futures[index++] = ThreadUtil.getThreadPool().submit(r);
            }
            try {
                boolean success = latch.await(Config.ELECT_RATE.toMillis(), TimeUnit.MILLISECONDS);
                if (!success) {
                    Arrays.stream(futures).forEach(e -> e.cancel(true));
                    LOGGER.error("elect timeout, cancel all task");
                }
            } catch (InterruptedException exception) {
                Arrays.stream(futures).forEach(e -> e.cancel(true));
                LOGGER.error("elect interrupted, cancel all task");
            }
            if (fail.getAcquire()) {
                Arrays.stream(futures).forEach(e -> e.cancel(true));
                LOGGER.error("Elect failed, cancel all task");
            }
            int mostTicketNum = BigDecimal.valueOf(this.allNodeAddresses.size() / 2D).setScale(0, RoundingMode.UP).intValue();
            if (ticket.getAcquire() >= mostTicketNum) {
                LOGGER.info("Elect successfully，leader info: {}", this.address);
                this.currentTerm = this.currentTerm + 1;
                this.leaderAddress = this.address;
                this.role = Role.LEADER;
                this.nextHeartbeatTime = -1;
                ThreadUtil.getThreadPool().submit(this.heartbeatTask);
            } else {
                LOGGER.info("Elect failed, expect ticket is {}, but received {}", mostTicketNum, ticket.get());
            }
            this.nextElectTime = this.nextElectTime();
        } finally {
            System.out.println("Elect spent time: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + "ms");
            this.writeLock.unlock();
        }

    }


    public static boolean isDead(SocketChannel channel) {
        return channel == null || !channel.isOpen() || !channel.isConnected();
    }

    public boolean isLeader() {
        return leaderAddress != null && leaderAddress.equals(this.address) && role == Role.LEADER;
    }

    public Response handle(Request request) {
        if (request == null) {
            return null;
        }
        if (!this.start) {
            return null;
        }

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
        return response;
    }

    public Set<NodeAddress> allNodeAddressExcludeMe() {
        HashSet<NodeAddress> nodeAddresses = new HashSet<>(this.allNodeAddresses);
        nodeAddresses.remove(this.address);
        return nodeAddresses;
    }

    // randomize 0-150ms, avoid two nodes elect at the same
    public long nextElectTime() {
        return System.currentTimeMillis() + Config.ELECT_RATE.toMillis() + ThreadLocalRandom.current().nextInt(100);// randomize 0--100 ms
    }

}
