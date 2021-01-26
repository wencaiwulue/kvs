package util;

import io.netty.buffer.ByteBuf;
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

import java.net.InetSocketAddress;

public class WebSocketTestClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketTestClientHandler.class);

    private final WebSocketClientHandshaker handShaker;
    private ChannelPromise handshakeFuture;
    private final InetSocketAddress remote;

    public WebSocketTestClientHandler(WebSocketClientHandshaker handShaker, InetSocketAddress remote) {
        this.handShaker = handShaker;
        this.remote = remote;
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
        LOGGER.info("WebSocket Client disconnected!");
        RpcClient.getConnection()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() == ctx.channel())
                .findFirst()
                .ifPresent(entry -> LOGGER.info("with server: " + entry.getKey().toString()));
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        Channel ch = ctx.channel();
        if (!handShaker.isHandshakeComplete()) {
            try {
                handShaker.finishHandshake(ch, (FullHttpResponse) msg);
                LOGGER.info("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                LOGGER.error("WebSocket Client failed to connect");
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
            LOGGER.info("{} --> {} message: {}", remote.getPort(), WebSocketServer.LOCAL_ADDRESS.getPort(), object.toString());
            if (object instanceof Response) {
                LOGGER.info("{} --> {} message: {}", remote.getPort(), WebSocketServer.LOCAL_ADDRESS.getPort(), object.toString());
            } else if (object instanceof Request) {
                LOGGER.warn("Test client don't need to receive request");
                ctx.close();
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