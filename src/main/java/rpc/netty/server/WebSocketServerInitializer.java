package rpc.netty.server;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import rpc.model.requestresponse.Response;
import rpc.netty.handler.HeartbeatHandler;
import rpc.netty.handler.WebSocketServerHandler;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class WebSocketServerInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;
    private final InetSocketAddress localAddress;
    private final Function<Object, Response> function;

    public WebSocketServerInitializer(SslContext sslCtx, InetSocketAddress localAddress, Function<Object, Response> function) {
        this.sslCtx = sslCtx;
        this.localAddress = localAddress;
        this.function = function;
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslCtx != null) {
            pipeline.addLast(sslCtx.newHandler(ch.alloc()));
        }
        pipeline.addLast(new HttpServerCodec());
        pipeline.addLast(new HttpObjectAggregator(65536));
        pipeline.addLast(new IdleStateHandler(2, 3, 5, TimeUnit.SECONDS));
        pipeline.addLast(new HeartbeatHandler());
        pipeline.addLast(new WebSocketServerHandler(localAddress, function));
    }
}
