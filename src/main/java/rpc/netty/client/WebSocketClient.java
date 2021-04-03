package rpc.netty.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rpc.model.requestresponse.Response;
import rpc.netty.handler.WebSocketClientHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public final class WebSocketClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketClient.class);

    // all client connections will use one event loop, actually, can use WebSocketServer.java's bossGroup, but for performance, use singe one
    private static final EventLoopGroup EVENT_LOOP = new NioEventLoopGroup(1);

    private final Function<Object, Response> function;
    private final InetSocketAddress localAddress;

    public WebSocketClient( InetSocketAddress localAddress,Function<Object, Response> function) {
        this.function = function;
        this.localAddress = localAddress;
    }

    public Channel doConnection(InetSocketAddress remoteAddress) {
        try {
            SslContext sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            AtomicReference<WebSocketClientHandler> clientHandlerRef = new AtomicReference<>();

            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(EVENT_LOOP)
                    .channel(NioSocketChannel.class)
                    .handler(new WebSocketClientInitializer(sslCtx, remoteAddress, clientHandlerRef, localAddress, function));
            Channel channel = bootstrap.connect(remoteAddress.getHostName(), remoteAddress.getPort()).sync().channel();
            clientHandlerRef.get().getHandshakeFuture().sync();
            return channel;
        } catch (Exception e) {
            LOGGER.error("Server {} is unhealthily, please check the machine status", remoteAddress.getPort());
            return null;
        }
    }
}
