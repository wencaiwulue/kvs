package raft;

import com.google.common.collect.Sets;
import db.config.Config;
import db.core.DB;
import db.core.LogDB;
import db.core.StateMachine;
import db.core.storage.impl.MapStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.config.Constant;
import raft.enums.Role;
import raft.processor.Processor;
import rpc.model.requestresponse.AppendEntriesRequest;
import rpc.model.requestresponse.AppendEntriesResponse;
import rpc.model.requestresponse.ErrorResponse;
import rpc.model.requestresponse.InstallSnapshotRequest;
import rpc.model.requestresponse.InstallSnapshotResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.model.requestresponse.SynchronizeStateRequest;
import rpc.model.requestresponse.VoteRequest;
import rpc.model.requestresponse.VoteResponse;
import rpc.netty.RpcClient;
import util.ThreadUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    // cluster leader address
    private volatile NodeAddress leaderAddress;
    // all nodes address, include current node
    private Set<NodeAddress> allNodeAddresses;

    private StateMachine stateMachine;
    private LogDB logEntries;
    // default role is follower
    private Role role = Role.FOLLOWER;

    private volatile long committedIndex = 0;
    private volatile long lastAppliedIndex = 0;
    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    private volatile Map<NodeAddress, Long> nextIndex; // Reinitialized after election
    // for each server, index of highest log entry known to be replicated on server
    private volatile Map<NodeAddress, Long> matchIndex;// Reinitialized after election
    // persistent storage currentTerm and lastVoteFor
    private volatile int currentTerm;
    // current term whether voted or not
    private volatile NodeAddress lastVoteFor;

    // first elect can be longer than normal election time
    private volatile long nextElectTime = this.nextElectTime() + TimeUnit.SECONDS.toMillis(5);
    private volatile long nextHeartbeatTime;

    private ReadWriteLock lock = new ReentrantReadWriteLock();
    private Lock readLock = this.lock.readLock();
    private Lock writeLock = this.lock.writeLock();

    private List<Processor> processors = new ArrayList<>(10);

    private Runnable heartbeatTask;
    private Runnable electTask;

    public static Node of(NodeAddress nodeAddress) {
        return new Node(nodeAddress, Sets.newHashSet(nodeAddress));
    }

    public Node(NodeAddress localAddress, Set<NodeAddress> allNodeAddresses) {
        this.localAddress = localAddress;
        this.allNodeAddresses = allNodeAddresses;
        this.stateMachine = new StateMachine(new DB(Config.DB_DIR));
        this.logEntries = new LogDB(Config.LOG_DIR);
        // initialized to leader last log index + 1
        this.nextIndex = this.allNodeAddresses.stream().collect(Collectors.toMap(e -> e, e -> this.logEntries.getLastLogIndex() + 1));
        // initialized to 0, increases monotonically
        this.matchIndex = this.allNodeAddresses.stream().collect(Collectors.toMap(e -> e, e -> 0L));

        // read from persistent storage, write before return result to client
        this.currentTerm = this.logEntries.getCurrentTerm();
        this.lastVoteFor = this.logEntries.getLastVoteFor();
    }

    {
        // use SPI to load business processors
        ServiceLoader.load(Processor.class).iterator().forEachRemaining(this.processors::add);
    }

    @Override
    public void start() {
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
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.electTask, 0, Constant.ELECT_RATE.toMillis(), TimeUnit.MILLISECONDS);

        this.heartbeatTask = () -> {
            if (!this.start) {
                return;
            }

            if (this.nextHeartbeatTime > System.currentTimeMillis()) {
                return;
            }

            if (!this.isLeader()) {
                return;
            }

            // if current node role is leader, then needs to send heartbeat to other, tell them i am boss, i rule anything !!!
            for (NodeAddress remote : this.allNodeAddressExcludeMe()) {
                Consumer<Response> consumer = res -> {
                    if (res != null) {
                        LOGGER.debug("{} --> {}, receive heartbeat response", remote.getPort(), this.localAddress.getPort());
                    }
                    // install snapshot
                    // 这里约定的是如果心跳包回复ErrorResponse, 说明是out too much的节点，需要install snapshot
                    if (res instanceof ErrorResponse) {
                        InstallSnapshotResponse snapshotResponse = (InstallSnapshotResponse) RpcClient.doRequest(remote, new InstallSnapshotRequest(this.currentTerm, this.localAddress, 0, 0, 0, null, true));
                        if (snapshotResponse == null || snapshotResponse.getTerm() < this.currentTerm) {
                            LOGGER.error("Install snapshot error, should retry or not ?");
                        }
                    } else if (res instanceof AppendEntriesResponse) {
                        AppendEntriesResponse response = (AppendEntriesResponse) res;
                        if (response.getTerm() > this.currentTerm) {
                            this.role = Role.FOLLOWER;
                            this.currentTerm = response.getTerm();
                        }
                    }
                };
                AppendEntriesRequest request = AppendEntriesRequest.builder()
                        .term(this.currentTerm)
                        .leaderId(this.localAddress)
                        .prevLogIndex(0)
                        .prevLogTerm(0)
                        .entries(Collections.emptyList())
                        .leaderCommit(this.committedIndex)
                        .build();
                Long followerNextIndex = this.getNextIndex().getOrDefault(remote, this.logEntries.getLastLogIndex() + 1);
                if (followerNextIndex == this.logEntries.getLastLogIndex() + 1) {
                    request.setPrevLogIndex(this.logEntries.getLastLogIndex());
                    request.setPrevLogTerm(this.logEntries.getLastLogTerm());
                } else {
                    LogEntry logEntry = this.logEntries.get(followerNextIndex - 1);
                    if (logEntry == null) {
                        LOGGER.warn("Log entry is null");
                    } else {
                        request.setPrevLogIndex(logEntry.getIndex());
                        request.setPrevLogIndex(logEntry.getTerm());
                    }
                }
                RpcClient.doRequestAsync(remote, request, consumer);
            }
            this.nextElectTime = this.nextElectTime();
            this.nextHeartbeatTime = System.currentTimeMillis() + Constant.HEARTBEAT_RATE.toMillis();
            LOGGER.debug("Delay elect successfully");
        };
        ThreadUtil.getScheduledThreadPool().scheduleAtFixedRate(this.heartbeatTask, 0, Constant.HEARTBEAT_RATE.toMillis(), TimeUnit.MILLISECONDS);
        this.start = true;
    }

    @Override
    public void shutdown() {
        this.start = false;
        if (this.stateMachine.getDb() instanceof MapStorage) {
            ((MapStorage<?, ?>) this.stateMachine.getDb()).writeDataToDisk();
        }
        ThreadUtil.getThreadPool().shutdown();
        ThreadUtil.getScheduledThreadPool().shutdown();
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    private void elect() {
        // only one node
        if (this.allNodeAddresses.size() < 3) {
            LOGGER.info("Can't create a cluster, current node number is {}, cluster minimum node number is 3", this.allNodeAddresses.size());
            return;
        }

        LOGGER.info("Start elect...");
        long start = System.nanoTime();
        this.writeLock.lock();
        try {
            this.currentTerm++;
            AtomicInteger ticket = new AtomicInteger(1); // vote for self
            AtomicBoolean fail = new AtomicBoolean(false);
            int size = this.allNodeAddressExcludeMe().size();
            CountDownLatch latch = new CountDownLatch(size);
            List<Integer> requestIds = new ArrayList<>(size);
            for (NodeAddress addr : this.allNodeAddressExcludeMe()) {
                Consumer<Response> consumer = res -> {
                    try {
                        if (res == null) {
                            LOGGER.error("{} --> {}, Vote response is null", addr.getPort(), this.localAddress.getPort());
                            return;
                        }
                        if (fail.get()) {
                            LOGGER.warn("{} --> {}, Already failed", addr.getPort(), this.localAddress.getPort());
                            return;
                        }

                        VoteResponse response = (VoteResponse) res;
                        LOGGER.debug("{} --> {}, vote response: {}", addr.getPort(), this.localAddress.getPort(), response.toString());
                        if (response.getTerm() > this.currentTerm) {
                            this.role = Role.FOLLOWER;
                            this.currentTerm = response.getTerm();
                            fail.set(true);
                            return;
                        }
                        if (this.role.equals(Role.CANDIDATE) && response.isVoteGranted() && response.getTerm() == this.currentTerm) {
                            ticket.incrementAndGet();
                        } else {
                            LOGGER.error("{} --> {}, Not vote for me", addr.getPort(), this.localAddress.getPort());
                        }
                    } finally {
                        latch.countDown();
                    }
                };
                VoteRequest voteRequest = new VoteRequest(this.currentTerm, this.localAddress, this.logEntries.getLastLogIndex(), this.logEntries.getLastLogTerm());
                requestIds.add(voteRequest.getRequestId());
                RpcClient.doRequestAsync(addr, voteRequest, consumer);
            }
            try {
                boolean success = latch.await(Constant.ELECT_RATE.toMillis(), TimeUnit.MILLISECONDS);
                if (!success) {
                    // should i to waiting for other node's response or not ?
                    AtomicBoolean mayInterruptIfRunning = new AtomicBoolean(true);
                    if (ticket.getAcquire() > 1) {
                        mayInterruptIfRunning.set(false);
                    }
                    requestIds.forEach(e -> RpcClient.cancelRequest(e, mayInterruptIfRunning.get()));
                    LOGGER.error("elect timeout, cancel all task");
                }
            } catch (InterruptedException ex) {
                requestIds.forEach(e -> RpcClient.cancelRequest(e, true));
                LOGGER.error("elect interrupted, cancel all task");
            }
            if (fail.getAcquire()) {
                requestIds.forEach(e -> RpcClient.cancelRequest(e, true));
                LOGGER.error("Elect failed, cancel all task");
                return;
            }
            int mostTicketNum = (this.allNodeAddresses.size() / 2) + 1;
            if (ticket.getAcquire() >= mostTicketNum) {
                LOGGER.info("Elect successfully, term:{}, leader info: {}", this.currentTerm, this.localAddress);
                this.leaderAddress = this.localAddress;
                this.role = Role.LEADER;
                this.nextHeartbeatTime = -1;
                ThreadUtil.getThreadPool().submit(() -> {
                    this.heartbeatTask.run();
                    this.allNodeAddressExcludeMe().forEach(e -> RpcClient.doRequestAsync(e, new SynchronizeStateRequest(this.getLocalAddress(), this.getAllNodeAddresses()), null));
                    this.leaderReplicateLog();
                });
                // Reinitialized after election
                this.nextIndex = this.allNodeAddresses.stream().collect(Collectors.toMap(e -> e, e -> this.logEntries.getLastLogIndex() + 1));
                this.matchIndex = this.allNodeAddresses.stream().collect(Collectors.toMap(e -> e, e -> 0L));
            } else {
                LOGGER.info("Elect failed, expect ticket is {}, but received {}", mostTicketNum, ticket.get());
            }
            this.nextElectTime = this.nextElectTime();
        } finally {
            LOGGER.debug("Elect spent time: {}ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            this.writeLock.unlock();
        }
    }

    public void leaderReplicateLog() {
        if (!isLeader()) return;
        // If last log index ≥ nextIndex for a follower:
        //   send AppendEntries RPC with log entries starting at nextIndex
        //     If successful: update nextIndex and matchIndex for follower
        //     If AppendEntries fails because of log inconsistency:
        //       decrement nextIndex and retry
        for (NodeAddress address : this.allNodeAddressExcludeMe()) {
            long followerNextIndex = this.nextIndex.getOrDefault(address, this.logEntries.getLastLogIndex() + 1);
            long followerMatchIndex = this.matchIndex.getOrDefault(address, 0L);
            if (this.logEntries.getLastLogIndex() >= followerNextIndex) {
                while (true) {
                    LogEntry logEntry = this.logEntries.get(followerNextIndex - 1);
                    AppendEntriesRequest request = AppendEntriesRequest.builder()
                            .leaderId(this.localAddress)
                            .leaderCommit(this.getCommittedIndex())
                            .term(this.currentTerm)
                            .prevLogIndex(Optional.ofNullable(logEntry).map(LogEntry::getIndex).orElse(0L))
                            .prevLogTerm(Optional.ofNullable(logEntry).map(LogEntry::getTerm).orElse(0))
                            .entries(this.logEntries.getRange(followerNextIndex, this.logEntries.getLastLogIndex() + 1))
                            .build();
                    AppendEntriesResponse response = (AppendEntriesResponse) RpcClient.doRequest(address, request);
                    if (response != null && response.isSuccess()) {
                        LOGGER.info("Leader replicate log to follower: {} successfully", address);
                        this.nextIndex.put(address, followerNextIndex);
                        this.matchIndex.put(address, followerMatchIndex);
                        break;
                    }
                    // If retry too much time, think about InstallSnapshot
                    followerNextIndex--;
                    LOGGER.info("Leader replicate log to follower: {} retrying", address);
                    if (followerNextIndex < 0) {
                        LOGGER.warn("Leader replicate log to follower: {} failed", address);
                        break;
                    }
                }
            } else {
                LOGGER.info("Don't needs to replicate log");
            }
        }
        // If there exists an N such that N > commitIndex, a majority
        // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
        // set commitIndex = N
        int majority = this.allNodeAddresses.size() / 2 + 1;
        this.matchIndex.values()
                .stream()
                .sorted()
                .collect(Collectors.groupingBy(e -> e))
                .entrySet()
                .stream()
                .filter(e -> e.getKey() > this.committedIndex)
                .filter(e -> e.getValue().size() >= majority)
                .max(Comparator.comparingLong(Map.Entry::getKey))
                .map(Map.Entry::getKey)
                .ifPresent(N -> {
                    LOGGER.info("Found a N: {}, meet the requirements", N);
                    this.committedIndex = N;
                });
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
            response.setRequestId(request.getRequestId());
        }

        // write before return result to client, read while init node
        this.logEntries.setCurrentTerm(this.currentTerm);
        this.logEntries.setLastVoteFor(this.lastVoteFor);
        return response;
    }

    public Set<NodeAddress> allNodeAddressExcludeMe() {
        HashSet<NodeAddress> nodeAddresses = new HashSet<>(this.allNodeAddresses);
        nodeAddresses.remove(this.localAddress);
        return nodeAddresses;
    }

    // randomize 0-100ms, avoid two nodes elect at the same
    public long nextElectTime() {
        return System.currentTimeMillis() + Constant.ELECT_RATE.toMillis() + ThreadLocalRandom.current().nextInt(100);
    }

    /*
     * notify follower apply log to statemachine
     * issue: if network split occurs between after leader apply log to statemachine and notify peer to apply log to statemachine
     * how to avoid this issue ?
     * */
    public void applyLogToStatemachine(List<LogEntry> entries) {
        // apply log to statemachine
        this.stateMachine.applyLog(entries);
        // push commitIndex
        long lastLogIndex = this.logEntries.getLastLogIndex();
        this.setCommittedIndex(lastLogIndex);
        this.setLastAppliedIndex(lastLogIndex);

        if (this.isLeader()) {
            // Second round, notify peer to apply log to statemachine
            AppendEntriesRequest request = AppendEntriesRequest.builder()
                    .term(this.currentTerm)
                    .leaderId(this.localAddress)
                    .prevLogIndex(lastLogIndex)
                    .prevLogTerm(this.logEntries.getLastLogTerm())
                    .entries(entries)
                    .leaderCommit(this.committedIndex)
                    .build();
            for (NodeAddress remote : this.allNodeAddressExcludeMe()) {
                this.matchIndex.put(remote, lastLogIndex);
                this.nextIndex.put(remote, lastLogIndex + 1);
                RpcClient.doRequestAsync(remote, request, response -> {
                    if (response != null && ((AppendEntriesResponse) response).isSuccess()) {
                        this.getNextIndex().put(remote, lastLogIndex + 1);
                        this.getMatchIndex().put(remote, lastLogIndex);
                    } else {
                        LOGGER.error("Notify follower to apply log to statemachine occurs error, so try to replicate log");
                        this.leaderReplicateLog();
                    }
                });
            }
        }
    }
}
