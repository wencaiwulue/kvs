package rpc.netty.pub;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    private static final Map<InetSocketAddress, Channel> CONNECTIONS = new ConcurrentHashMap<>();
    private static final Map<Integer, Response> RESPONSE_MAP = new ConcurrentHashMap<>();
    private static final Map<Integer, CountDownLatch> RESPONSE_MAP_LOCK = new ConcurrentHashMap<>();
    private static final ArrayBlockingQueue<SocketRequest> REQUEST_TASK = new ArrayBlockingQueue<>(10 * 1000 * 1000);

    static {
        ThreadUtil.getThreadPool().execute(RpcClient::writeRequest);
    }

    public static void addConnection(InetSocketAddress remoteAddress, Channel channel) {
        CONNECTIONS.put(remoteAddress, channel);
    }

    public static Map<InetSocketAddress, Channel> getConnection() {
        return ImmutableMap.copyOf(CONNECTIONS);
    }

    public static void addResponse(int requestId, Response response) {
        RESPONSE_MAP.put(requestId, response);
        CountDownLatch latch = RESPONSE_MAP_LOCK.remove(requestId);
        if (latch != null) {
            latch.countDown();
        }
    }

    private static Channel getConnection(InetSocketAddress remoteAddress) {
        if (remoteAddress == null) {
            return null;
        }
        Supplier<Boolean> supplier = () -> !CONNECTIONS.containsKey(remoteAddress)
                || !CONNECTIONS.get(remoteAddress).isOpen()
                || !CONNECTIONS.get(remoteAddress).isActive()
                || !CONNECTIONS.get(remoteAddress).isRegistered();

        if (supplier.get()) {
            synchronized (remoteAddress.toString().intern()) {
                if (supplier.get()) {
                    WebSocketClient.doConnection(remoteAddress);
                }
            }
        }
        return CONNECTIONS.get(remoteAddress);
    }

    public static Response doRequest(NodeAddress remoteAddress, final Request request) {
        return doRequest(remoteAddress.getSocketAddress(), request);
    }

    public static Response doRequest(InetSocketAddress remoteAddress, final Request request) {
        if (remoteAddress == null) {
            return null;
        }

        CountDownLatch latch = new CountDownLatch(1);
        SocketRequest socketRequest = new SocketRequest(remoteAddress, request);
        try {
            REQUEST_TASK.put(socketRequest);
            RESPONSE_MAP_LOCK.put(request.requestId, latch);
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage());
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
        return RESPONSE_MAP.remove(request.requestId);
    }

    private static void writeRequest() {
        //noinspection InfiniteLoopStatement
        while (true) {
            SocketRequest socketRequest = null;
            try {
                socketRequest = REQUEST_TASK.take();
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage());
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
