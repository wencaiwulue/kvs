package util;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import rpc.model.requestresponse.Request;

import java.net.InetSocketAddress;
import java.net.URI;

public class NettyClientTest {
    public static void main(InetSocketAddress address, Request request) throws Exception {
        String url = "wss://" + address.getHostName() + ":" + address.getPort() + "/";
        URI uri = new URI(url);
        SslContext sslCtx = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            DefaultHttpHeaders httpHeaders = new DefaultHttpHeaders();
            httpHeaders.add("localhost", "127.0.0.1");
            httpHeaders.add("localport", 8888);
            WebSocketClientHandshaker handshake = WebSocketClientHandshakerFactory.newHandshaker(uri, WebSocketVersion.V13, "diy-protocol", true, httpHeaders);
            WebSocketTestClientHandler handler = new WebSocketTestClientHandler(handshake, new InetSocketAddress(uri.getHost(), uri.getPort()));

            Bootstrap bootstrap = new Bootstrap();
            bootstrap
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                protected void initChannel(SocketChannel ch) {
                                    ChannelPipeline p = ch.pipeline();
                                    p.addLast(sslCtx.newHandler(ch.alloc(), uri.getHost(), uri.getPort()));
                                    p.addLast(new HttpClientCodec());
                                    p.addLast(new HttpObjectAggregator(8192));
                                    p.addLast(WebSocketClientCompressionHandler.INSTANCE);
                                    p.addLast(handler);
                                }
                            });

            Channel ch = bootstrap.connect(uri.getHost(), uri.getPort()).sync().channel();
            handler.getHandshakeFuture().sync();
            byte[] bytes = FSTUtil.getBinaryConf().asByteArray(request);
            ch.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(bytes)))
                    .addListener(ChannelFutureListener.CLOSE);
            ch.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }
}
