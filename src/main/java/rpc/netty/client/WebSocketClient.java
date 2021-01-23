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
import rpc.netty.server.HeartBeatHandler;
import rpc.netty.server.WebSocketServer;
import rpc.netty.config.Constant;
import rpc.netty.pub.RpcClient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public final class WebSocketClient {
    static final EventLoopGroup EVENT_LOOP = new NioEventLoopGroup(1);

    public static void doConnection(InetSocketAddress address) {
        try {
            URI uri = new URI("wss", null, address.getHostName(), address.getPort(), "/", null, null);
            SslContext sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

            AtomicReference<WebSocketClientHandler> ref = new AtomicReference<>();
            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(EVENT_LOOP)
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
                                    headers.add(Constant.LOCALHOST, WebSocketServer.SELF.getHostName());
                                    headers.add(Constant.LOCALPORT, WebSocketServer.SELF.getPort());
                                    WebSocketClientHandshaker handshake = WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, "diy-protocol", true, headers);
                                    ref.set(new WebSocketClientHandler(handshake, address));
                                    pipeline.addLast(ref.get());
                                }
                            });

            Channel channel = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            ref.get().getHandshakeFuture().sync();
            RpcClient.addConnection(address, channel);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
