package rpc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.Node;
import raft.NodeAddress;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import util.FSTUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class RpcClientForTest {
    private static final Logger LOGGER = LogManager.getLogger(RpcClient.class);

    // address -> list<channel>, 支持多个连接
    private static final int CORE_SIZE = 20;

    private static final ConcurrentHashMap<NodeAddress, Connect> CONNECTIONS = new ConcurrentHashMap<>();// 节点之间的连接

    public static Selector selector;// 这个selector处理的是请求的回包

    private static final ConcurrentHashMap<Integer, Response> RESPONSE_MAP = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, CountDownLatch> RESPONSE_MAP_LOCK = new ConcurrentHashMap<>();
    private static final LinkedBlockingDeque<SocketRequest> REQUEST_TASK = new LinkedBlockingDeque<>();
    private static final Thread WRITE_REQUEST_TASK = new Thread(RpcClientForTest::writeRequest);

    static {
        try {
            selector = Selector.open();
            ThreadUtil.getThreadPool().submit(RpcClientForTest::readResponse);
            WRITE_REQUEST_TASK.start();
        } catch (IOException e) {
            LOGGER.error("at the beginning error occurred, shutting down...", e);
            Runtime.getRuntime().exit(-1);
        }
    }

    public static Response doRequest(NodeAddress remote, final Request request) {
        if (remote == null /*|| !remote.alive*/) return null;

        CountDownLatch latch = new CountDownLatch(1);
        REQUEST_TASK.addLast(new SocketRequest(remote, request));
        RESPONSE_MAP_LOCK.put(request.requestId, latch);
        LockSupport.unpark(WRITE_REQUEST_TASK);

        try {
            boolean b = latch.await(5, TimeUnit.SECONDS);
            if (!b) {
                System.out.printf("b:%s\n", b);
                return null;
            }
        } catch (InterruptedException e) {
            LOGGER.error(e);
            System.out.println("error");
        }
        return RESPONSE_MAP.remove(request.requestId);
    }

    private static void writeRequest() {
        while (true) {
            while (!REQUEST_TASK.isEmpty()) {
                SocketRequest socketRequest = REQUEST_TASK.peek();
                if (socketRequest != null) {
                    boolean success = false;
                    SocketChannel channel = getConnection(socketRequest.address);
                    if (channel != null && socketRequest.address.alive) {
                        try {
                            int write = channel.write(FSTUtil.asArrayWithLength(socketRequest.request));
                            if (write <= 0) throw new IOException("魔鬼！！！");
                            success = true;
                            REQUEST_TASK.poll();
                        } catch (IOException e) {
                            LOGGER.error(e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
                            success = false;
                        }
                    }
                    recycle(socketRequest.address, channel);

                    if (!success) {
                        socketRequest.address.alive = false;
                    }
                }
            }
            LockSupport.park();
        }
    }

    @SuppressWarnings("InfiniteLoopStatement")
    private static void readResponse() {
        Consumer<SelectionKey> action = key -> {
            if (key.isReadable()) {
                SocketChannel channel = (SocketChannel) key.channel();
                ByteBuffer contentLen = ByteBuffer.allocate(4);
                try {
                    int read = channel.read(contentLen);
                    if (read > 0) {
                        contentLen.flip();
                        int len = contentLen.getInt();
                        if (len > 0) {
                            System.out.println("len: " + len);
                            ByteBuffer result = ByteBuffer.allocate(len);
                            if (len == channel.read(result)) {
                                Response response = (Response) FSTUtil.getConf().asObject(result.array());
                                if (response != null) {
                                    RESPONSE_MAP.put(response.requestId, response);
                                    RESPONSE_MAP_LOCK.get(response.requestId).countDown();
                                    RESPONSE_MAP_LOCK.remove(response.requestId);
                                }
                            }
                        }
                    } else if (read < 0) {
                        if (key.isValid()) {
                            key.cancel();
                        }
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    if (e.getMessage().contains("An existing connection was forcibly closed by the remote host")) {
                        key.cancel();
                        try {
                            channel.close();
                        } catch (Exception ignored) {
                        }
                    }
                }

            }
        };

        while (true) {
            try {
                selector.selectNow(action);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static SocketChannel getConnection(NodeAddress remote) {
        if (remote == null) return null;

        if (!CONNECTIONS.containsKey(remote) || CONNECTIONS.get(remote).available.isEmpty() || Node.isDead(CONNECTIONS.get(remote).available.peek())) {
            synchronized (remote.toString().intern()) {
                if (!CONNECTIONS.containsKey(remote) || CONNECTIONS.get(remote).available.isEmpty() || Node.isDead(CONNECTIONS.get(remote).available.peek())) {

                    CONNECTIONS.putIfAbsent(remote, new Connect(CORE_SIZE));

                    if (CONNECTIONS.get(remote).haveSpace()) {// 如果没超过core size
                        try {
                            SocketChannel channel = SocketChannel.open(remote.getSocketAddress());
                            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                            channel.configureBlocking(false);
                            channel.register(selector, SelectionKey.OP_READ);
                            CONNECTIONS.computeIfPresent(remote, (nodeAddress, connect) -> {
                                connect.available.add(channel);
                                connect.aliveNum.incrementAndGet();
                                return connect;
                            });

                            remote.alive = true;
                        } catch (ConnectException e) {
                            LOGGER.error("remote:{}, 连接失败, message:{}。", remote.getSocketAddress().getPort(), e.getMessage());
                            remote.alive = false;
                        } catch (IOException e) {
                            LOGGER.error(e);
                            remote.alive = false;
                        }
                    }
                }
            }
        }
        try {
            return CONNECTIONS.get(remote).available.take();// 阻塞取
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void recycle(NodeAddress remote, SocketChannel channel) {
        if (channel == null || !channel.isConnected() || !channel.isOpen()) {// 如果已经失效，就把当前持有数减少，方便下次新建
            CONNECTIONS.computeIfPresent(remote, (nodeAddress, connect) -> {
                connect.aliveNum.decrementAndGet();
                connect.available.remove(channel);
                return connect;
            });
        } else {
            CONNECTIONS.computeIfPresent(remote, (nodeAddress, connect) -> {
                connect.available.add(channel);
                return connect;
            });
        }
    }


    private static class SocketRequest {
        public NodeAddress address;
        public Request request;

        private SocketRequest(NodeAddress address, Request request) {
            this.address = address;
            this.request = request;
        }
    }

    private static class Connect {
        public BlockingQueue<SocketChannel> available;
        public AtomicInteger aliveNum;

        public Connect(int max) {
            available = new ArrayBlockingQueue<>(max);
            aliveNum = new AtomicInteger(0);
        }

        public boolean haveSpace() {
            return aliveNum.get() < CORE_SIZE;
        }
    }

}
