package rpc.netty.client;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import rpc.model.requestresponse.Response;
import rpc.netty.config.Constant;
import rpc.netty.handler.HeartbeatHandler;
import rpc.netty.handler.WebSocketClientHandler;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class WebSocketClientInitializer extends ChannelInitializer<SocketChannel> {
    private final SslContext sslCtx;
    private final InetSocketAddress remoteAddress;
    private final AtomicReference<WebSocketClientHandler> clientHandlerRef;

    private final InetSocketAddress localAddress;
    private final Function<Object, Response> function;

    public WebSocketClientInitializer(SslContext sslCtx, InetSocketAddress remoteAddress, AtomicReference<WebSocketClientHandler> clientHandlerRef, InetSocketAddress localAddress, Function<Object, Response> function) {
        this.sslCtx = sslCtx;
        this.remoteAddress = remoteAddress;
        this.clientHandlerRef = clientHandlerRef;
        this.localAddress = localAddress;
        this.function = function;
    }

    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast(sslCtx.newHandler(ch.alloc(), remoteAddress.getHostName(), remoteAddress.getPort()));
        pipeline.addLast(new HttpClientCodec());
        pipeline.addLast(new HttpObjectAggregator(8192));
        pipeline.addLast(WebSocketClientCompressionHandler.INSTANCE);
        pipeline.addLast(new WebSocket13FrameEncoder(true));
        pipeline.addLast(new WebSocket13FrameDecoder(false, true, 65536));
        pipeline.addLast(new IdleStateHandler(2, 3, 5, TimeUnit.SECONDS));
        pipeline.addLast(new HeartbeatHandler());
        DefaultHttpHeaders headers = new DefaultHttpHeaders();
        // tell remote server, who am i
        headers.add(Constant.LOCALHOST, localAddress.getHostName());
        headers.add(Constant.LOCALPORT, localAddress.getPort());
        URI uri = new URI("wss", null, remoteAddress.getHostName(), remoteAddress.getPort(), Constant.WEBSOCKET_PATH, null, null);
        WebSocketClientHandshaker handshake = WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, Constant.WEBSOCKET_PROTOCOL, true, headers);
        clientHandlerRef.set(new WebSocketClientHandler(handshake, remoteAddress, localAddress, function));
        pipeline.addLast(clientHandlerRef.get());
    }
}
