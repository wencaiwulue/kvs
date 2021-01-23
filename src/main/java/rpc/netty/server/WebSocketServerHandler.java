package rpc.netty.server;

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
import rpc.netty.pub.RpcClient;
import util.FSTUtil;

import java.net.InetSocketAddress;

@ChannelHandler.Sharable
public class WebSocketServerHandler extends SimpleChannelInboundHandler<Object> {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerHandler.class);

    private WebSocketServerHandshaker handShaker;
    private InetSocketAddress remote;

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
        System.out.printf("localhost: %s, localport: %s\n", localhost, localport);
        remote = new InetSocketAddress(localhost, localport);
        // Handshake
        String uri = "wss://" + req.headers().get(HttpHeaderNames.HOST) + Constant.WEBSOCKET_PATH;
        System.out.println(uri);
        WebSocketServerHandshakerFactory wsFactory = new WebSocketServerHandshakerFactory(uri, "diy-protocol", true, 5 * 1024 * 1024);
        handShaker = wsFactory.newHandshaker(req);
        if (handShaker == null) {
            WebSocketServerHandshakerFactory.sendUnsupportedVersionResponse(ctx.channel());
        } else {
            handShaker.handshake(ctx.channel(), req);
            RpcClient.addConnection(remote, ctx.channel());
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
//            System.out.println("Server receive ping");
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
            System.out.printf("%s --> %s message: %s\n", remote.getPort(), WebSocketServer.SELF.getPort(), object.toString());
            if (object instanceof Response) {
                RpcClient.addResponse(((Response) object).requestId, (Response) object);
            } else if (object instanceof Request) {
                Response response = WebSocketServer.NODE.handle((Request) object);
                System.out.printf("%s --> %s message: %s\n", remote.getPort(), WebSocketServer.PORT, object.toString());
                if (response != null) {
                    System.out.printf("%s --> %s reponse: %s\n", WebSocketServer.PORT, remote.getPort(), response.toString());
                    byte[] byteArray = FSTUtil.getBinaryConf().asByteArray(response);
                    ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(byteArray)))
                            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                } else {
                    System.out.println("server handler, response is empty ??");
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}