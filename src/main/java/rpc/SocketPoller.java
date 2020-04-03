package rpc;

import lombok.Data;
import raft.Node;
import rpc.model.Request;
import rpc.model.Response;
import thread.FSTUtil;
import thread.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author naison
 * @since 4/3/2020 20:13
 */
@Data
public class SocketPoller implements Runnable {
    private InetSocketAddress addr;
    private List<InetSocketAddress> peerAddr;
    private Node node;
    private volatile ConcurrentHashMap<InetSocketAddress, SocketChannel> connect = new ConcurrentHashMap<>();

    // server 之间的通讯端口是addr的port + 1000
    public SocketPoller(InetSocketAddress addr, List<InetSocketAddress> peerAddr) {
        this.addr = new InetSocketAddress(addr.getHostString(), addr.getPort() + 1000);
        this.peerAddr = peerAddr.stream().peek(e -> new InetSocketAddress(e.getHostString(), e.getPort() + 1000)).collect(Collectors.toList());
    }

    @Override
    public void run() {
        Runnable r = () -> {
            try {
                ServerSocketChannel socket = ServerSocketChannel.open().bind(addr);
                while (true) {
                    SocketChannel accept = socket.accept();
                    InetSocketAddress remoteAddress = (InetSocketAddress) accept.getRemoteAddress();
                    connect.putIfAbsent(new InetSocketAddress(remoteAddress.getHostString(), remoteAddress.getPort() - 1000), accept);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        };
        ThreadUtil.getPool().execute(r);

        Runnable r1 = () -> {
            ArrayDeque<SocketChannel> socketChannels = new ArrayDeque<>(connect.values());
            while (true) {
                while (!socketChannels.isEmpty()) {
                    SocketChannel first = socketChannels.pollFirst();
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    try {
                        int read = first.read(buffer);
                        if (read > 0) {
                            Object o = FSTUtil.getConf().asObject(buffer.array());
                            if (o instanceof Request) {
                                this.node.handle((Request) o, first);
                            } else {
                                throw new RuntimeException("这是不允许的");
                            }
                            socketChannels.add(first);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    socketChannels.push(first);
                }
            }
        };
        ThreadUtil.getPool().execute(r1);
    }

    private SocketChannel getServerConnection(InetSocketAddress remote) {
        connect.computeIfAbsent(remote, address -> {
            try {
                SocketChannel channel = SocketChannel.open(new InetSocketAddress(remote.getHostString(), remote.getPort() + 1000));
                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                return channel;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        });
        return connect.get(remote);
    }

    public Response request(InetSocketAddress remote, Request request) {
        try {
            SocketChannel serverConnection = getServerConnection(remote);
            if (serverConnection != null) {
                serverConnection.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(request)));
                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                while (serverConnection.read(byteBuffer) <= 0) {
                }
                return (Response) FSTUtil.getConf().asObject(byteBuffer.array());
            }
            return null;
        } catch (IOException e) {
            return null;
        }
    }
}
