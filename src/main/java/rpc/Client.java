package rpc;

import rpc.model.Request;
import rpc.model.RequestTypeEnum;
import thread.FSTUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author naison
 * @since 3/14/2020 15:46
 */
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        ExecutorService pool = new ThreadPoolExecutor(100, 200, 2, TimeUnit.MINUTES, new ArrayBlockingQueue<>(1000000));
        AtomicInteger error = new AtomicInteger(0);
        int n = 10;
        CountDownLatch c = new CountDownLatch(n);
        long time = System.nanoTime();
        for (int i = 0; i < n; i++) {
            Runnable r = () -> {
                try {
                    SocketChannel channel = SocketChannel.open(new InetSocketAddress("127.0.0.1", 8000));
                    channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Request(RequestTypeEnum.Vote, false, Map.of()))));
                    ByteBuffer buffer = ByteBuffer.allocate(1024);
                    channel.read(buffer);
                    System.out.println(new String(buffer.array()));
                    channel.close();
                } catch (Exception e) {
                    error.addAndGet(1);
                } finally {
                    c.countDown();
                }
            };
            pool.execute(r);
        }
        c.await();
        System.out.printf("共创建%s个链接，耗时%s毫秒, 出错个数为%s", n, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - time), error.get());
    }
}
