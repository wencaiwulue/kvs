package raft;

import com.google.common.collect.Sets;
import db.core.Config;
import db.core.DB;
import db.core.LogDB;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.enums.Role;
import raft.processor.Processor;
import rpc.model.requestresponse.*;
import rpc.netty.RpcClient;
import util.ThreadUtil;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
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
@Getter
@Setter
@ToString
public class Node implements INode {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    private volatile boolean start;

    // current node address
    private NodeAddress localAddress;
    // all nodes address, include current node
    private Set<NodeAddress> allNodeAddresses;

    private DB db;
    private LogDB logdb;
    // default role is follower
    private Role role = Role.FOLLOWER;

    private volatile long committedIndex;
    private AtomicLong lastAppliedIndex = new AtomicLong(0);
    private volatile int lastAppliedTerm = 0;
    private volatile long nextIndex;

    // first elect can be longer than normal election time
    private volatile long nextElectTime = this.nextElectTime() + TimeUnit.SECONDS.toMillis(5);
    private volatile long nextHeartbeatTime /*= System.currentTimeMillis() + this.heartBeatRate*/;

    private volatile int currentTerm = 0;
    // current term whether voted or not
    private volatile NodeAddress lastVoteFor;
    // cluster leader address
    private volatile NodeAddress leaderAddress;

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = this.lock.readLock();
    private Lock writeLock = this.lock.writeLock();

    private List<Processor> processors = new ArrayList<>(10);

    private Runnable heartbeatTask;
    private Runnable electTask;

    public static Node of(NodeAddress nodeAddress) {
        InetSocketAddress p8001 = new InetSocketAddress("127.0.0.1", 8001);
        InetSocketAddress p8002 = new InetSocketAddress("127.0.0.1", 8002);
        InetSocketAddress p8003 = new InetSocketAddress("127.0.0.1", 8003);
        NodeAddress p1 = new NodeAddress(p8001);
        NodeAddress p2 = new NodeAddress(p8002);
        NodeAddress p3 = new NodeAddress(p8003);
        return new Node(nodeAddress, Sets.newHashSet(p1, p2, p3));
    }

    public Node(NodeAddress localAddress, Set<NodeAddress> allNodeAddresses) {
        this.localAddress = localAddress;
        this.allNodeAddresses = allNodeAddresses;
        this.db = new DB(Config.DB_DIR);
        this.logdb = new LogDB(Config.LOG_DIR);
        this.nextIndex = this.logdb.getLastLogIndex() + 1;
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
                    Response response = RpcClient.doRequest(remote, new AppendEntriesRequest(Collections.emptyList(), this.localAddress, this.currentTerm, this.lastAppliedTerm, this.lastAppliedIndex.intValue(), this.committedIndex));
                    // install snapshot
                    if (response instanceof ErrorResponse) {// todo 这里约定的是如果心跳包回复ErrorResponse, 说明是out的节点，需要安装更新
                        long size = 0;
                        try {
                            // todo 这里需要根据index确定是哪一个文件，并且index要做到全局递增，这个怎么办？
                            size = FileChannel.open(this.logdb.getFile().get(0).toPath(), StandardOpenOption.READ).size();
                        } catch (ClosedChannelException e) {
                            LOGGER.error("who close the channel ?!!!");
                        } catch (IOException e) {
                            LOGGER.error(e.getMessage());
                        }
                        InstallSnapshotResponse snapshotResponse = (InstallSnapshotResponse) RpcClient.doRequest(remote, new InstallSnapshotRequest(this.localAddress, this.currentTerm, this.logdb.getDir().toString(), size));
                        if (snapshotResponse == null || !snapshotResponse.isSuccess()) {
                            LOGGER.error("Install snapshot error, should retry?");
                        }
                    }
                    if (response != null) {
                        LOGGER.error("{} --> {}, receive heartbeat response", remote.getSocketAddress().getPort(), this.localAddress.getSocketAddress().getPort());
                    }
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
                        VoteResponse response = (VoteResponse) RpcClient.doRequest(addr, new VoteRequest(this.localAddress, this.currentTerm, this.logdb.getLastLogIndex(), this.logdb.getLastLogTerm()));
                        if (response != null) {
                            LOGGER.error("{} --> {}, vote response: {}", addr.getSocketAddress().getPort(), this.localAddress.getSocketAddress().getPort(), response.toString());
                            if (this.role.equals(Role.CANDIDATE) && response.isGrant() && response.getTerm() == this.currentTerm) {
                                ticket.incrementAndGet();
                            } else if (response.getTerm() > this.currentTerm) {
                                this.role = Role.FOLLOWER;
                                this.currentTerm = response.getTerm();
                                fail.set(true);
                            } else {
                                LOGGER.error("{} --> {}, no vote response", addr.getSocketAddress().getPort(), this.localAddress.getSocketAddress().getPort());
                            }
                        } else {
                            LOGGER.error("{} --> {}, null vote response", addr.getSocketAddress().getPort(), this.localAddress.getSocketAddress().getPort());
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
                    // should i to waiting for other node's response or not ?
                    if (ticket.getAcquire() > 1) {
                    }
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
                LOGGER.info("Elect successfully，leader info: {}", this.localAddress);
                this.currentTerm = this.currentTerm + 1;
                this.leaderAddress = this.localAddress;
                this.role = Role.LEADER;
                this.nextHeartbeatTime = -1;
                ThreadUtil.getThreadPool().submit(this.heartbeatTask);
            } else {
                LOGGER.info("Elect failed, expect ticket is {}, but received {}", mostTicketNum, ticket.get());
            }
            this.nextElectTime = this.nextElectTime();
        } finally {
            LOGGER.info("Elect spent time: {}ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            this.writeLock.unlock();
        }

    }

    public boolean isLeader() {
        return this.localAddress.equals(this.leaderAddress) && this.role == Role.LEADER;
    }

    public Response handle(Request request) {
        if (!this.start || request == null) {
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
        nodeAddresses.remove(this.localAddress);
        return nodeAddresses;
    }

    // randomize 0-100ms, avoid two nodes elect at the same
    public long nextElectTime() {
        return System.currentTimeMillis() + Config.ELECT_RATE.toMillis() + ThreadLocalRandom.current().nextInt(100);
    }

}
