package rpc.netty.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Node;
import raft.NodeAddress;
import util.ThreadUtil;

import java.net.InetSocketAddress;

public final class WebSocketServer {
    public static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    public static final int PORT = Integer.parseInt(System.getenv("port"));
    public static final InetSocketAddress SELF = new InetSocketAddress("127.0.0.1", PORT);
    public static final Node NODE = Node.of(new NodeAddress(SELF));

    public static void main(String[] args) throws Exception {
        // Configure SSL.
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        final SslContext sslCtx =
                SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerInitializer(sslCtx));

            Channel ch = bootstrap.bind(PORT).sync().channel();
            LOGGER.info("Server started on port(s): {} (websocket)", PORT);
            ThreadUtil.getThreadPool().execute(NODE);
            ch.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
