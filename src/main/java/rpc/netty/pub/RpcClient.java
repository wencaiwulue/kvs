package rpc.netty.pub;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import raft.NodeAddress;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.client.WebSocketClient;
import util.FSTUtil;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class RpcClient {
    private static final Map<InetSocketAddress, Channel> CONNECTIONS = new ConcurrentHashMap<>(); // 主节点于各个简单的链接
    private static final Map<Integer, Response> RESPONSE_MAP = new ConcurrentHashMap<>();
    private static final Map<Integer, CountDownLatch> RESPONSE_MAP_LOCK = new ConcurrentHashMap<>();
    private static final ArrayBlockingQueue<SocketRequest> REQUEST_TASK = new ArrayBlockingQueue<>(10 * 1000 * 1000);

    static {
        ThreadUtil.getThreadPool().execute(RpcClient::writeRequest);
    }

    public static void addConnection(InetSocketAddress k, Channel v) {
        CONNECTIONS.put(k, v);
    }

    public static Map<InetSocketAddress, Channel> getConnection() {
        return ImmutableMap.copyOf(CONNECTIONS);
    }

    public static void addResponse(int k, Response v) {
        RESPONSE_MAP.put(k, v);
        CountDownLatch latch = RESPONSE_MAP_LOCK.get(k);
        if (latch != null) {
            latch.countDown();
        }
    }

    private static Channel getConnection(InetSocketAddress remote) {
        if (remote == null) return null;
        Supplier<Boolean> supplier = () -> !CONNECTIONS.containsKey(remote)
                || !CONNECTIONS.get(remote).isOpen()
                || !CONNECTIONS.get(remote).isActive()
                || !CONNECTIONS.get(remote).isRegistered();

        if (supplier.get()) {
            synchronized (remote.toString().intern()) {
                if (supplier.get()) {
                    WebSocketClient.doConnection(remote);
                }
            }
        }
        return CONNECTIONS.get(remote);
    }

    public static Response doRequest(NodeAddress remote, final Request request) {
        return doRequest(remote.getSocketAddress(), request);
    }

    public static Response doRequest(InetSocketAddress remote, final Request request) {
        if (remote == null) return null;

        CountDownLatch latch = new CountDownLatch(1);
        SocketRequest socketRequest = new SocketRequest(remote, request);
        try {
            REQUEST_TASK.put(socketRequest);
            RESPONSE_MAP_LOCK.put(request.requestId, latch);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            boolean a = latch.await(300, TimeUnit.MILLISECONDS);
            if (!a) {
                socketRequest.cancelled = true;
                return null;
            }
        } catch (InterruptedException e) {
            socketRequest.cancelled = true;
            return null;
        }
        Response response = RESPONSE_MAP.remove(request.requestId);
        System.out.printf("response info: %s\n", FSTUtil.getJsonConf().asJsonString(response));
        return response;
    }

    private static void writeRequest() {
        //noinspection InfiniteLoopStatement
        while (true) {
            SocketRequest socketRequest = null;
            try {
                socketRequest = REQUEST_TASK.take();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (socketRequest != null && !socketRequest.cancelled) {
                boolean success = false;
                int retry = 0;
                while (retry++ < 3) {
                    Channel channel = getConnection(socketRequest.address);
                    if (channel != null) {
                        byte[] byteArray = FSTUtil.getBinaryConf().asByteArray(socketRequest.request);
                        channel.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(byteArray)))
                                .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
                        success = true;
                        break;
                    }
                }
            }
        }
    }

    private static class SocketRequest {
        public InetSocketAddress address;
        public Request request;
        public boolean cancelled;

        private SocketRequest(InetSocketAddress address, Request request) {
            this.address = address;
            this.request = request;
            this.cancelled = false;
        }
    }
}
