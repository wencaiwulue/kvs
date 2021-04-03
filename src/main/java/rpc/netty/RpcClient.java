package rpc.netty;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class RpcClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(RpcClient.class);

    // hold all connections
    private static final Map<InetSocketAddress, Channel> CONNECTIONS = new ConcurrentHashMap<>();
    // the request needs to write out
    private static final ArrayBlockingQueue<SocketRequest> REQUEST_TASK = new ArrayBlockingQueue<>(10 * 1000 * 1000);
    // synchronize mode
    private static final Map<Integer, Response> RESPONSE_MAP = new ConcurrentHashMap<>();
    private static final Map<Integer, CountDownLatch> RESPONSE_MAP_LOCK = new ConcurrentHashMap<>();
    // asynchronize mode
    private static final Map<Integer, Consumer<Response>> RESPONSE_CONSUMER = new ConcurrentHashMap<>();
    private static final Map<Integer, Future<?>> RUNNING = new ConcurrentHashMap<>();

    private final InetSocketAddress local;
    private final Function<Object, Response> function;

    public RpcClient(InetSocketAddress local, Function<Object, Response> function) {
        this.local = local;
        this.function = function;
        ThreadUtil.getThreadPool().execute(this::writeRequest);
    }

    public static void addConnection(InetSocketAddress remoteAddress, Channel channel) {
        CONNECTIONS.put(remoteAddress, channel);
    }

    public Map<InetSocketAddress, Channel> getConnection() {
        return ImmutableMap.copyOf(CONNECTIONS);
    }

    public void addResponse(int requestId, Response response) {
        RESPONSE_MAP.put(requestId, response);
        CountDownLatch latch = RESPONSE_MAP_LOCK.remove(requestId);
        Consumer<Response> consumer = RESPONSE_CONSUMER.remove(requestId);
        if (latch != null) {
            latch.countDown();
        } else if (consumer != null) {
            RUNNING.put(requestId, ThreadUtil.getThreadPool().submit(() -> consumer.accept(response)));
        }
    }

    private Channel getConnection(InetSocketAddress remoteAddress) {
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
                    Channel channel = new WebSocketClient(local, function).doConnection(remoteAddress);
                    if (channel != null) {
                        addConnection(remoteAddress, channel);
                    }
                }
            }
        }
        return CONNECTIONS.get(remoteAddress);
    }

    // todo delete request from request queue
    public void cancelRequest(int requestId, boolean mayInterruptIfRunning) {
        Consumer<Response> consumer = RESPONSE_CONSUMER.remove(requestId);
        Future<?> remove = RUNNING.remove(requestId);
        if (remove != null) {
            if (!remove.isDone() && !remove.isCancelled()) {
                remove.cancel(mayInterruptIfRunning);
            }
        }
    }

    public void doRequestAsync(NodeAddress remoteAddress, Request request, Consumer<Response> nextTodo) {
        if (remoteAddress == null) {
            return;
        }
        SocketRequest socketRequest = new SocketRequest(remoteAddress.getSocketAddress(), request);
        try {
            REQUEST_TASK.put(socketRequest);
            if (nextTodo != null) {
                RESPONSE_CONSUMER.put(request.getRequestId(), nextTodo);
            }
        } catch (InterruptedException e) {
            LOGGER.error("add async request error, info: {}", e.getMessage());
        }
    }

    public Response doRequest(NodeAddress remoteAddress, Request request) {
        return doRequest(remoteAddress.getSocketAddress(), request);
    }

    public Response doRequest(InetSocketAddress remoteAddress, final Request request) {
        if (remoteAddress == null) {
            return null;
        }

        CountDownLatch latch = new CountDownLatch(1);
        SocketRequest socketRequest = new SocketRequest(remoteAddress, request);
        try {
            REQUEST_TASK.put(socketRequest);
            RESPONSE_MAP_LOCK.put(request.getRequestId(), latch);
        } catch (InterruptedException e) {
            LOGGER.error("Add sync request error, info: {}", e.getMessage());
        }

        try {
            boolean success = latch.await(300, TimeUnit.MILLISECONDS);
            if (!success) {
                socketRequest.cancelled = true;
                return null;
            }
        } catch (InterruptedException e) {
            socketRequest.cancelled = true;
            return null;
        }
        return RESPONSE_MAP.remove(request.getRequestId());
    }

    private void writeRequest() {
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
                if (!success) {
                    LOGGER.warn("Write out request error, already retry three times, should retry or not ? request info: {}", socketRequest);
                }
            }
        }
    }

    @Getter
    @Setter
    @ToString
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
