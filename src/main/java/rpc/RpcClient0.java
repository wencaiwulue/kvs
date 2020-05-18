//package rpc;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import raft.Node;
//import raft.NodeAddress;
//import rpc.model.requestresponse.Request;
//import rpc.model.requestresponse.Response;
//import util.FSTUtil;
//import util.ThreadUtil;
//
//import java.io.IOException;
//import java.net.ConnectException;
//import java.net.StandardSocketOptions;
//import java.nio.ByteBuffer;
//import java.nio.channels.SelectionKey;
//import java.nio.channels.Selector;
//import java.nio.channels.SocketChannel;
//import java.util.concurrent.ArrayBlockingQueue;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.LinkedBlockingDeque;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.LockSupport;
//import java.util.function.Consumer;
//
///**
// * @author naison
// * @since 3/14/2020 15:46
// */
//public class RpcClient1 {
//    private static final Logger log = LogManager.getLogger(RpcClient.class);
//
//    // address -> list<channel>, 支持多个连接
//    private static final int coreSize = 20;
//
//    private static final ConcurrentHashMap<NodeAddress, Connect> connections = new ConcurrentHashMap<>();// 节点之间的连接
//
//    public static Selector selector;// 这个selector处理的是请求的回包
//
//    private static final ConcurrentHashMap<Integer, Response> responseMap = new ConcurrentHashMap<>();
//    private static final LinkedBlockingDeque<SocketRequest> requestTask = new LinkedBlockingDeque<>();
//    private static final Thread writeRequestTask = new Thread(RpcClient::writeRequest);
//
//    static {
//        try {
//            selector = Selector.open();
//            ThreadUtil.getThreadPool().execute(RpcClient::readResponse);
//            writeRequestTask.start();
//        } catch (IOException e) {
//            log.error("at the beginning error occurred, shutting down...", e);
//            Runtime.getRuntime().exit(-1);
//        }
//    }
//
//    public static Response doRequest(NodeAddress remote, final Request request) {
//        if (remote == null /*|| !remote.alive*/) return null;
//
//        requestTask.addLast(new SocketRequest(remote, request));
//        LockSupport.unpark(writeRequestTask);
//
//        // optimize
//        int m = 1;
//        int t = 200; // 400ms 就超时了
//        while (m++ < t) {
//            if (!responseMap.containsKey(request.requestId) && remote.alive) {
//                ThreadUtil.sleep(2);
//            } else {
//                break;
//            }
//        }
//        return responseMap.remove(request.requestId);
//    }
//
//    private static void writeRequest() {
//        while (true) {
//            while (!requestTask.isEmpty()) {
//                SocketRequest socketRequest = requestTask.peek();
//                if (socketRequest != null) {
//                    boolean success = false;
//                    SocketChannel channel = getConnection(socketRequest.address);
//                    if (channel != null && socketRequest.address.alive) {
//                        try {
//                            int write = channel.write(FSTUtil.asArrayWithLength(socketRequest.request));
//                            if (write <= 0) throw new IOException("魔鬼！！！");
//                            success = true;
//                            requestTask.poll();
//                        } catch (IOException e) {
//                            log.error(e); // 这里可能出现的情况是对方关闭了channel，该怎么办呢？
//                            success = false;
//                        }
//                    }
//                    recycle(socketRequest.address, channel);
//
//                    if (!success) {
//                        socketRequest.address.alive = false;
//                    }
//                }
//            }
//            LockSupport.park();
//        }
//    }
//
//    @SuppressWarnings("InfiniteLoopStatement")
//    private static void readResponse() {
//        Consumer<SelectionKey> action = key -> {
//            if (key.isReadable()) {
//                SocketChannel channel = (SocketChannel) key.channel();
//                ByteBuffer contentLen = ByteBuffer.allocate(4);
//                try {
//                    int read = channel.read(contentLen);
//                    if (read > 0) {
//                        contentLen.flip();
//                        int len = contentLen.getInt();
//                        if (len > 0) {
//                            System.out.println("len: " + len);
//                            ByteBuffer result = ByteBuffer.allocate(len);
//                            if (len == channel.read(result)) {
//                                Response response = (Response) FSTUtil.getConf().asObject(result.array());
//                                if (response != null) {
//                                    responseMap.put(response.requestId, response);
//                                }
//                            }
//                        }
//                    } else if (read < 0) {
//                        if (key.isValid()) {
//                            key.cancel();
//                        }
//                    }
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                    if (e.getMessage().contains("An existing connection was forcibly closed by the remote host")) {
//                        key.cancel();
//                        try {
//                            channel.close();
//                        } catch (Exception ignored) {
//                        }
//                    }
//                }
//
//            }
//        };
//
//        while (true) {
//            try {
//                selector.selectNow(action);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//
//    public static SocketChannel getConnection(NodeAddress remote) {
//        if (remote == null) return null;
//
//        if (!connections.containsKey(remote) || connections.get(remote).available.isEmpty() || Node.isDead(connections.get(remote).available.peek())) {
//            synchronized (remote.toString().intern()) {
//                if (!connections.containsKey(remote) || connections.get(remote).available.isEmpty() || Node.isDead(connections.get(remote).available.peek())) {
//
//                    connections.putIfAbsent(remote, new Connect(coreSize));
//
//                    if (connections.get(remote).haveSpace()) {// 如果没超过core size
//                        try {
//                            SocketChannel channel = SocketChannel.open(remote.getSocketAddress());
//                            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
//                            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
//                            channel.configureBlocking(false);
//                            channel.register(selector, SelectionKey.OP_READ);
//                            connections.computeIfPresent(remote, (nodeAddress, connect) -> {
//                                connect.available.add(channel);
//                                connect.aliveNum.incrementAndGet();
//                                return connect;
//                            });
//
//                            remote.alive = true;
//                        } catch (ConnectException e) {
//                            log.error("remote:{}, 连接失败, message:{}。", remote.getSocketAddress().getPort(), e.getMessage());
//                            remote.alive = false;
//                        } catch (IOException e) {
//                            log.error(e);
//                            remote.alive = false;
//                        }
//                    }
//                }
//            }
//        }
//        try {
//            return connections.get(remote).available.take();// 阻塞取
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//            return null;
//        }
//    }
//
//    public static void recycle(NodeAddress remote, SocketChannel channel) {
//        if (channel == null || !channel.isConnected() || !channel.isOpen()) {// 如果已经失效，就把当前持有数减少，方便下次新建
//            connections.computeIfPresent(remote, (nodeAddress, connect) -> {
//                connect.aliveNum.decrementAndGet();
//                connect.available.remove(channel);
//                return connect;
//            });
//        } else {
//            connections.computeIfPresent(remote, (nodeAddress, connect) -> {
//                connect.available.add(channel);
//                return connect;
//            });
//        }
//    }
//
//
//    private static class SocketRequest {
//        public NodeAddress address;
//        public Request request;
//
//        private SocketRequest(NodeAddress address, Request request) {
//            this.address = address;
//            this.request = request;
//        }
//    }
//
//    private static class Connect {
//        public BlockingQueue<SocketChannel> available;
//        public AtomicInteger aliveNum;
//
//        public Connect(int max) {
//            available = new ArrayBlockingQueue<>(max);
//            aliveNum = new AtomicInteger(0);
//        }
//
//        public boolean haveSpace() {
//            return aliveNum.get() < coreSize;
//        }
//    }
//
//}
