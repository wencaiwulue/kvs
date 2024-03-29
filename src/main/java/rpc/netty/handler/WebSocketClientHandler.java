package rpc.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.RpcClient;
import rpc.netty.server.WebSocketServer;
import util.FSTUtil;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.function.Function;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketClientHandler.class);

    private final WebSocketClientHandshaker handShaker;
    private ChannelPromise handshakeFuture;
    private final InetSocketAddress remote;
    private final InetSocketAddress localAddress;
    private final Function<Object, Response> function;

    public WebSocketClientHandler(WebSocketClientHandshaker handShaker, InetSocketAddress remote, InetSocketAddress localAddress, Function<Object, Response> function) {
        this.handShaker = handShaker;
        this.remote = remote;
        this.localAddress = localAddress;
        this.function = function;
    }

    public ChannelFuture getHandshakeFuture() {
        return handshakeFuture;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        handshakeFuture = ctx.newPromise();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        handShaker.handshake(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        LOGGER.debug("WebSocket Client disconnected!, with server: {}", this.remote);
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        Channel ch = ctx.channel();
        if (!handShaker.isHandshakeComplete()) {
            try {
                handShaker.finishHandshake(ch, (FullHttpResponse) msg);
                LOGGER.debug("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                LOGGER.warn("WebSocket Client failed to connect");
                handshakeFuture.setFailure(e);
            }
            return;
        }

        if (msg instanceof FullHttpResponse) {
            FullHttpResponse response = (FullHttpResponse) msg;
            throw new IllegalStateException(
                    "Unexpected FullHttpResponse (getStatus="
                            + response.status()
                            + ", content="
                            + response.content().toString(CharsetUtil.UTF_8)
                            + ')');
        }

        WebSocketFrame frame = (WebSocketFrame) msg;
        if (frame instanceof PingWebSocketFrame) {
            ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()))
                    .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            return;
        }
        if (frame instanceof CloseWebSocketFrame) {
            ch.close();
            return;
        }
        if (frame instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame socketFrame = (BinaryWebSocketFrame) frame;
            ByteBuf buffer = socketFrame.content().retain();
            byte[] bytes = new byte[buffer.capacity()];
            buffer.readBytes(bytes);
            Object object = FSTUtil.getBinaryConf().asObject(bytes);
            LOGGER.debug("{} --> {} message: {}", remote.getPort(), localAddress.getPort(), object.toString());
            if (object instanceof Response) {
                  function.apply( object);
            } else if (object instanceof Request) {
                ThreadUtil.getThreadPool().submit(() -> {
                    Response response = function.apply(object);
                    if (response != null) {
                        byte[] byteArray = FSTUtil.getBinaryConf().asByteArray(response);
                        ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(byteArray)))
                                .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
                    }
                });
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error(cause.getMessage());
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
