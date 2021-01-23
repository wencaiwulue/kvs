package rpc.netty.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.util.CharsetUtil;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;
import rpc.netty.pub.RpcClient;
import rpc.netty.server.WebSocketServer;
import util.FSTUtil;

import java.net.InetSocketAddress;

public class WebSocketClientHandler extends SimpleChannelInboundHandler<Object> {

    private final WebSocketClientHandshaker handShaker;
    private ChannelPromise handshakeFuture;
    private final InetSocketAddress remote;

    public WebSocketClientHandler(WebSocketClientHandshaker handShaker, InetSocketAddress remote) {
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
        System.out.println("WebSocket Client disconnected!");
        RpcClient.getConnection()
                .entrySet()
                .stream()
                .filter(e -> e.getValue() == ctx.channel())
                .findFirst()
                .ifPresent(entry -> System.out.println("with server: " + entry.getKey().toString()));
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) {
        Channel ch = ctx.channel();
        if (!handShaker.isHandshakeComplete()) {
            try {
                handShaker.finishHandshake(ch, (FullHttpResponse) msg);
                System.out.println("WebSocket Client connected!");
                handshakeFuture.setSuccess();
            } catch (WebSocketHandshakeException e) {
                System.out.println("WebSocket Client failed to connect");
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
            System.out.printf("%s --> %s message: %s\n", remote.getPort(), WebSocketServer.SELF.getPort(), object.toString());
            if (object instanceof Response) {
                RpcClient.addResponse(((Response) object).requestId, (Response) object);
            } else if (object instanceof Request) {
                Response response = WebSocketServer.NODE.handle((Request) object);
                if (response != null) {
                    byte[] byteArray = FSTUtil.getBinaryConf().asByteArray(response);
                    ctx.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(byteArray)))
                            .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
                } else {
                    System.out.println("client handler, response is empty ??");
                }
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        if (!handshakeFuture.isDone()) {
            handshakeFuture.setFailure(cause);
        }
        ctx.close();
    }
}
