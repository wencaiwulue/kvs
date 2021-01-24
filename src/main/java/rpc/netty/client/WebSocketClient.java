package rpc.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.netty.config.Constant;
import rpc.netty.server.HeartBeatHandler;
import rpc.netty.server.WebSocketServer;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class WebSocketClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketClient.class);

    // all client connections will use one event loop, actually, can use WebSocketServer.java's bossGroup, but for performance, use singe one
    private static final EventLoopGroup EVENT_LOOP = new NioEventLoopGroup(1);

    public static Channel doConnection(InetSocketAddress address) {
        try {
            URI uri = new URI("wss", null, address.getHostName(), address.getPort(), Constant.WEBSOCKET_PATH, null, null);
            SslContext sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            AtomicReference<WebSocketClientHandler> clientHandlerReference = new AtomicReference<>();
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(EVENT_LOOP)
                    .channel(NioSocketChannel.class)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ChannelPipeline pipeline = ch.pipeline();
                                    pipeline.addLast(sslCtx.newHandler(ch.alloc(), uri.getHost(), uri.getPort()));
                                    pipeline.addLast(new HttpClientCodec());
                                    pipeline.addLast(new HttpObjectAggregator(8192));
                                    pipeline.addLast(WebSocketClientCompressionHandler.INSTANCE);
                                    pipeline.addLast(new WebSocket13FrameEncoder(true));
                                    pipeline.addLast(new WebSocket13FrameDecoder(false, true, 65536));
                                    pipeline.addLast(new IdleStateHandler(2, 3, 5, TimeUnit.SECONDS));
                                    pipeline.addLast(new HeartBeatHandler());
                                    DefaultHttpHeaders headers = new DefaultHttpHeaders();
                                    // tell remote server, who am i
                                    headers.add(Constant.LOCALHOST, WebSocketServer.LOCAL_ADDRESS.getHostName());
                                    headers.add(Constant.LOCALPORT, WebSocketServer.LOCAL_ADDRESS.getPort());
                                    WebSocketClientHandshaker handshake = WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, Constant.WEBSOCKET_PROTOCOL, true, headers);
                                    clientHandlerReference.set(new WebSocketClientHandler(handshake, address));
                                    pipeline.addLast(clientHandlerReference.get());
                                }
                            });
            Channel channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            clientHandlerReference.get().getHandshakeFuture().sync();
            return channel;
        } catch (Exception e) {
            LOGGER.error("Server {} is unhealthily, please check the machine status", address.getPort());
            return null;
        }
    }
}
