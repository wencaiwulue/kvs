package rpc.netty.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.websocketx.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.config.Constant;
import rpc.netty.RpcClient;
import rpc.netty.server.WebSocketServer;
import util.FSTUtil;
import util.ThreadUtil;

import java.net.InetSocketAddress;
import java.util.function.Function;

@ChannelHandler.Sharable
public class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerHandler.class);

    private WebSocketServerHandshaker handShaker;
    private InetSocketAddress remoteAddress;
    private final InetSocketAddress localAddress;
    private final Function<Object, Response> function;

    public WebSocketServerHandler(InetSocketAddress localAddress, Function<Object, Response> consume) {
        this.localAddress = localAddress;
        this.function = consume;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpRequest) {
            this.handleHttpRequest(ctx, (FullHttpRequest) msg);
        } else if (msg instanceof WebSocketFrame) {
            this.handleWebSocketFrame(ctx, (WebSocketFrame) msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    private void handleHttpRequest(ChannelHandlerContext ctx, FullHttpRequest req) {
        String localhost = req.headers().get(Constant.LOCALHOST);
        int localport = req.headers().getInt(Constant.LOCALPORT);
        remoteAddress = new InetSocketAddress(localhost, localport);
        // Handshake
        String uri = "wss://" + req.headers().get(HttpHeaderNames.HOST) + Constant.WEBSOCKET_PATH;
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(uri, Constant.WEBSOCKET_PROTOCOL, true, 5 * 1024 * 1024);
        handShaker = wsFactory.newHandshaker(req);
        if (handShaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            handShaker.handshake(ctx.channel(), req);
            RpcClient.addConnection(remoteAddress, ctx.channel());
            LOGGER.debug("{} --> {} create a socket connection", localport, localAddress.getPort());
        }
    }

    private void handleWebSocketFrame(ChannelHandlerContext ctx, WebSocketFrame frame) {
        if (frame instanceof CloseWebSocketFrame) {
            handShaker.close(ctx.channel(), (CloseWebSocketFrame) frame.retain());
            return;
        }
        if (frame instanceof PingWebSocketFrame) {
            ctx.writeAndFlush(new PongWebSocketFrame(frame.content().retain()))
                    .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            return;
        }
        if (frame instanceof BinaryWebSocketFrame) {
            BinaryWebSocketFrame binaryFrame = (BinaryWebSocketFrame) frame;
            ByteBuf buffer = binaryFrame.content().retain();
            byte[] bytes = new byte[buffer.capacity()];
            buffer.readBytes(bytes);
            Object object = FSTUtil.getBinaryConf().asObject(bytes);
            if (object == null) {
                LOGGER.warn("object is null");
                return;
            }
            LOGGER.debug("{} --> {} message: {}", remoteAddress.getPort(), localAddress.getPort(), object.toString());
            if (object instanceof Response) {
                function.apply(object);
            } else if (object instanceof Request) {
                ThreadUtil.getThreadPool().submit(() -> {
                    Response response = function.apply(object);
                    LOGGER.debug("{} --> {} message: {}", remoteAddress.getPort(), localAddress.getPort(), object.toString());
                    if (response != null) {
                        LOGGER.debug("{} --> {} response: {}", localAddress.getPort(), remoteAddress.getPort(), response.toString());
                        byte[] byteArray = FSTUtil.getBinaryConf().asByteArray(response);
                        ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(byteArray)))
                                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                    }
                });
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("error info: {}", cause.getMessage());
        cause.printStackTrace();
        ctx.close();
    }
}
