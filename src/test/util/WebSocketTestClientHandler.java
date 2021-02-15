package util;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketHandshakeException;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.RpcClient;

public class WebSocketTestClientHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketTestClientHandler.class);

    private final WebSocketClientHandshaker handShaker;
    private ChannelPromise handshakeFuture;
    private Response response;

    public WebSocketTestClientHandler(WebSocketClientHandshaker handShaker) {
        this.handShaker = handShaker;
    }

    public ChannelFuture getHandshakeFuture() {
        return handshakeFuture;
    }

    public Response getResponse() {
        return response;
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
            LOGGER.info("message from server, message: {}", object);
            if (object instanceof Response) {
                LOGGER.info("response from server, response: {}", object.toString());
                this.response = (Response) object;
                ctx.close();
            } else if (object instanceof Request) {
                LOGGER.warn("Test client don't need to receive request, request: {}", object);
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.error(cause.getMessage());
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
