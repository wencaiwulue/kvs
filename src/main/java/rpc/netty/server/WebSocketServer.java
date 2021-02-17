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
import raft.INode;
import util.ThreadUtil;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;

public final class WebSocketServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServer.class);

    public static final InetSocketAddress LOCAL_ADDRESS = new InetSocketAddress("127.0.0.1", Integer.parseInt(System.getenv("port")));
    public static INode node;

    public static void main(INode iNode) {
        node = iNode;

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            // Configure SSL.
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            SslContext sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new WebSocketServerInitializer(sslCtx));

            Channel channel = bootstrap.bind(LOCAL_ADDRESS.getPort()).sync().channel();
            LOGGER.info("Server started on port(s): {} (websocket)", LOCAL_ADDRESS.getPort());
            ThreadUtil.getThreadPool().execute(iNode::start);
            channel.closeFuture().sync();
        } catch (CertificateException | InterruptedException | SSLException e) {
            LOGGER.error(e.getMessage());
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
