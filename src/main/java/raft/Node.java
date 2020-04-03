package raft;

import db.core.DB;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rpc.SocketPoller;
import rpc.model.Request;
import rpc.model.RequestTypeEnum;
import rpc.model.Response;
import thread.FSTUtil;
import thread.ThreadUtil;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static rpc.model.RequestTypeEnum.*;

/**
 * @author naison
 * @since 3/14/2020 19:05
 */
@Data
public class Node implements Runnable {
    private static final Logger log = LogManager.getLogger(Node.class);

    private Selector selector;

    private DB db = new DB();
    private InetSocketAddress address; // 本机的IP和端口信息
    private List<InetSocketAddress> peerAddress; //其它节点的IP和端口信息
    private ConcurrentHashMap<InetSocketAddress, SocketChannel> connects = new ConcurrentHashMap<>();// 主节点于各个简单的链接
    private SocketPoller socketPoller;
    private StateEnum state = StateEnum.Follower;// 默认是follower角色
    private Long timeout;//150ms -- 300ms randomized  超时时间,选举的时候，如果主节点挂了，则所有的节点开始timeout，然后最先timeout结束的节点变为candidate，
    // 参见竞选，然后发送竞选类型的请求，如果半数以上统一，则广播给所有人，
    // leader回一直发送心跳包，如果timeout后还没有发现心跳包来，就说明leader挂了，需要开始选举

    private AtomicLong l = new AtomicLong(0);// 用来生成提案编号使用

    private int term;// 第几任leader
    private volatile InetSocketAddress leaderAddr;//leader节点信息，因为所有的数据处理都需要leader来操作。

    public Node(InetSocketAddress address, List<InetSocketAddress> peerAddress, Selector selector) {
        this.address = address;
        this.peerAddress = peerAddress;
        this.selector = selector;
    }

    private void elect() {
        log.error("开始选举");
        AtomicInteger ai = new AtomicInteger(1);//先给自己投一票
        if (leaderAddr == null) {
            int i = new Random(47).nextInt(150);// 0-150ms
            try {
                Thread.sleep(i + 150);// 等待选举
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (leaderAddr == null) {
                state = StateEnum.Candidate;// 改变状态为candidate
                List<Future<Response>> list = new ArrayList<>(peerAddress.size());
                for (InetSocketAddress addr : peerAddress) {
                    int retry = 3;
//                    for (int p = 0; p < retry; p++) {
//                        SocketChannel socketChannel = socketPoller.getServerConnection(addr);
//                        if (socketChannel == null) {
////                        return null;// 可能主机卒了
//                            continue;
//                        }
//
//                        if (!socketChannel.isConnected() || !socketChannel.isOpen()) {
//                            log.error("到这里应该不会出现这样的情况，当前主机{}与主机{}的链接中断了", this.address, addr);
//                        }
//                    Callable<Response> c = () -> {
//                        try {// todo 这里写消息的时候，可以使用DirectByteBuffer实现零拷贝
//                            socketChannel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Request(Vote, true, Map.of("", "")))));
//                            ByteBuffer buffer = ByteBuffer.allocate(1024);
//                            int read = socketChannel.read(buffer);
//                            if (read > 0) {
//                                Response response = (Response) FSTUtil.getConf().asObject(buffer.array());
//                                log.error("收到从:{}的回包:{}", 1, response);
//                                if (response != null && response.getCode() == 200) {
//                                    ai.addAndGet(1);
//                                }
//                                break;
//                            }
                    Response response = this.socketPoller.request(addr, new Request(Vote, true, Map.of("", "")));
                    log.error("收到从:{}的回包:{}", 1, response);
                    if (response != null && response.getCode() == 200) {
                        ai.addAndGet(1);
                    }
                    break;

//                        } catch (IOException e) {
//                            log.error("可能是有的主机死掉了，有问题！！！", e);
//                        }
//                        return null;
//                    }
//                    };
//                    list.add(ThreadUtil.getPool().submit(c));
                }

//                for (Future<Response> future : list) {
//                    try {
//                        Response response = future.get(300, TimeUnit.MILLISECONDS);
//                        if (response != null && response.getCode() == 200) {
//                            ai.addAndGet(1);
//                        }
//                    } catch (Exception e) {
//                        log.error("等待选举的过程中出问题了，不用担心可能是因为有的机器宕机了，{}", e.getMessage());
//                    }
//                }
                if (ai.get() > Math.ceil(peerAddress.size() / 2D)) {// 超过半数了，我成功了
                    log.error("选举成功，选出leader了{}", this.address);
                    this.term = this.term + 1;
                    this.leaderAddr = this.address;
                } else {
                    log.error("选举失败");
                }
            }
        }
    }

    public void service(Request request, SocketChannel channel, Selector selector) {
        // todo curd
    }

    public void handle(Request request, SocketChannel channel) {
        if (request == null) {
            return;
        }

        switch (request.getType()) {
            case HeartBeat: {
                InetSocketAddress n = (InetSocketAddress) request.getBody().get("leader");

                log.error("已经收到来自leader的心跳包, 其中包含了主节点的信息，{}", n);
                if (n != null) {// 说明自己已经out了，需要更新主节点信息
                    this.leaderAddr = n;
                    try {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(Response.builder().message("pong").build())));// 先构成一次完整的rpc
                    } catch (ClosedChannelException e) {
                        log.error("这里的channel又失效了");
                    } catch (IOException e) {
                        log.error("心跳包回复失败。{}", e.getMessage());
                    }
                } else {
                    log.error("没有从心跳包中取到leader信息");
                }
                break;
            }
            case AddNewNode: {//todo
                try {
                    // todo 从un-commit中拿去日志，还要从主节点中拿取数据，也就是自己out事件里，世界的变化
//                    FileChannel f = FileChannel.open(Path.of("C:\\Users\\89570\\Documents\\Kvs file\\un-commit.txt"), Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
//                    MappedByteBuffer map = f.map(FileChannel.MapMode.READ_WRITE, 0, 1024);
//                    String s = new String(map.array());
                    if (!channel.isOpen() || !channel.isConnected()) {
                        log.error("AddNewNode话说这里为什么链接断了呢");
                    } else {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(Response.builder().type(AddNewNode).build())));// 先构成一次完整的rpc
                    }
                } catch (ClosedChannelException e) {
                    log.error("这里的channel又失效了");
                } catch (IOException e) {
                    log.error("回复失败。{}", e.getMessage());
                }
            }
            case Vote: {
                try {
                    if (!channel.isOpen() || !channel.isConnected()) {
                        log.error("Vote话说这里为什么链接断了呢");
                    } else {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(Response.builder().message("vote reply").build())));
                    }
                } catch (IOException e) {
                    log.error("vote回复失败。{}", e.getMessage());
                }
                break;
            }
            case tryAccept: {
                // todo 这里要写un-commit log
                if (leaderAddr == null) {
                    try {
                        elect();
                    } catch (Exception e) {
                        log.error("选举失败，{}", e.getMessage());
                    }
                }

                if (!leaderAddr.equals(this.address)) {
                    try {
                        SocketChannel socketChannel = connects.get(leaderAddr);
                        socketChannel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(request)));
                        ByteBuffer buffer = ByteBuffer.allocate(1024);
                        socketChannel.read(buffer); // 这里做了一次转发，但是转发还是一样，需要等回包，然后再返回给前端
                        channel.write(buffer);
                        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
                        channel.read(byteBuffer);
                        Response o = (Response) FSTUtil.getConf().asObject(byteBuffer.array());
                        // 这个是回包
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));// 这里返回给前端
                    } catch (IOException e) {
                        log.error("tryAccept1回复失败。{}", e.getMessage());
                    }
                } else {
                    try {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));
                    } catch (IOException e) {
                        log.error("tryAccept2回复失败。{}", e.getMessage());
                    }
                }
                break;
            }
            case doAccept: {
                try {
                    Optional<Map.Entry<String, Object>> entry = request.getBody().entrySet().stream().findFirst();
                    entry.ifPresent(stringObjectEntry -> db.set(stringObjectEntry.getKey(), stringObjectEntry.getValue()));
                    if (!channel.isOpen() || !channel.isConnected()) {
                        log.error("doAccept话说这里为什么链接断了呢");
                    } else {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));
                    }
                } catch (IOException e) {
                    log.error("vote回复失败。{}", e.getMessage());
                }
                break;
            }

            case Append: {
                if (isLeader()) {
                    String key = (String) request.getBody().getOrDefault("key", null);
                    Object o = db.get(key);
                    try {
                        if (!channel.isOpen() || !channel.isConnected()) {
                            log.error("Append话说这里为什么链接断了呢");
                        } else {
                            channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(
                                    new Response(200, "成功找到value", RequestTypeEnum.Append, Map.of(key, 1)))));
                        }
                    } catch (IOException e) {
                        log.error("追加数据错误, {}", e.getMessage());
                    }
                } else {
                    log.error("追加数据错误, 这台主机不是master ，但是收到了给master的信息，说明follower的leader信息有误，" +
                            "当前节点为：{}, 假leader为：{}", this.getAddress(), this.leaderAddr);
                }
                break;
            }
        }
    }

    private boolean isLeader() {
        return leaderAddr != null && leaderAddr.equals(this.address);
    }

    @Override
    public void run() {
        while (leaderAddr == null) {
            elect();
        }

        if (isLeader()) {// 如果自己是主领导，就需要给各个节点发送心跳包
            Runnable r = () -> {
                for (InetSocketAddress channel : connects.keySet()) {
                    if (channel.equals(this.address)) continue;// 自己不发

                    int retry = 3;
                    int i = 0;
                    Response response = this.socketPoller.request(channel, new Request(HeartBeat, true, Map.of("leader", this.address)));
                    while (i++ < retry) {
//                        SocketChannel socketChannel = getChannel(channel);
//                        if (socketChannel != null && socketChannel.isOpen() && socketChannel.isConnected()) {
//                            try {
//                                socketChannel.write(
//                                        ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Request(HeartBeat, true, Map.of("leader", this.address)))));
//                                ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
//                                int read = socketChannel.read(byteBuffer);
//                                if (read <= 0) {
//                                    continue;
//                                }
//                                Response o = (Response) FSTUtil.getConf().asObject(byteBuffer.array());// 这里其实拿到response没什么用，但关键是要自己立即读取，
//                                log.error("收到从follower的心跳回包.{}", o);
//                                if (AddNewNode == o.getType()) {// 说明之前的机器死掉了，现在活过来了
//                                    // todo 传输数据
//                                    socketChannel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Request(RequestTypeEnum.Append, true, Map.of()))));
//                                    ByteBuffer buffer = ByteBuffer.allocate(1024);
//                                    int read1 = socketChannel.read(buffer);// 这里可以判断是否有问题，
//                                    if (read1 < 0) {
//                                        continue;
//                                    }
//                                }
//                                if (!socketChannel.isRegistered()) {
//                                    socketChannel.register(selector, SelectionKey.OP_READ);
//                                }
//                                // 不然就会被read event捕获到，从而再次selectKey，就会乱
//                                log.error("已经通知了全球人");
//                                break;
//                            } catch (ClosedChannelException e) {
//                                log.error("");
//                            } catch (IOException e) {
//                                log.error("这里出错了。", e);
//                            }
//                        }
                    }
                }
            };
            ThreadUtil.getSchedulePool().scheduleAtFixedRate(r, 0, 150, TimeUnit.MILLISECONDS);// 每150ms心跳一下
        }
    }

    private SocketChannel getChannel(InetSocketAddress address) {
        if (!connects.containsKey(address) || (!connects.get(address).isOpen() || !connects.get(address).isConnected())) {
            synchronized (this) {
                if (!connects.containsKey(address) || (!connects.get(address).isOpen() || !connects.get(address).isConnected())) {
                    try {
                        SocketChannel channel = SocketChannel.open(address);
                        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                        channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        channel.configureBlocking(false);
                        connects.put(address, channel);
                    } catch (ConnectException e) {
//                        log.error("出错啦, 可能是有的主机死掉了，这个直接吞了，{}", e.getMessage());
                    } catch (IOException e) {
                        log.error(e);
                    }
                }
                return connects.get(address);
            }
        }
        return connects.get(address);
    }


    /**
     *
     *  Leader Election.主节点选举
     * if(follower没有听见来自leader的心跳){
     *     1,那么回自己生成一个随机的timeout事件
     *     2,开始倒计时
     *     if(倒计时结束还没有leader出来){
     *         1，自己就变成candidate。
     *         2，然后发送选举类型的请求包
     *         3，同步等待100毫秒
     *         if(timeout，则重试前判断是否有leader出来){
     *
     *         }else{
     *              1,是否超过半数，
     *              if(超过N/2){
     *                  则为leader, 这里迅速发送心跳包，并且通知各位修改leader节点信息
     *              }else{
     *                  判断是否有leader出来
     *                  if(没有){
     *                      重试
     *                  }else{
     *                      break;
     *                  }
     *              }
     *
     *         }
     *     }
     * }
     */

    /**
     * 更改数据
     * 1，从client接受请求，判断当前节点，是不是主节点
     * if(如果不是主节点 && 主节点存在){
     *     转发请求到主节点
     * }else if(如果不是主节点 && 主节点不存在){
     *      1，主节点选举。in
     *      2，转发到新选举出来的主节点
     * }else if(是主节点 && 主节点不存在){
     *      1，说明主节点已失效。
     *      2，清除主节点信息
     *      3，主节点选取
     *      4，转发到主节点
     * }else if(是主节点 && 主节点存在){
     *      1，写入log，但是不是提交状态
     *      2，rpc请求给所有的非follower节点，写入log, 但是不是提交状态
     *      3，rpc回包是否超过半数ok
     *      if(超过半数以上回包ok){
     *          1, 主节点提交写入日志，也就是undo log 或者是redo log
     *          2, 第二次rpc说是同志们都提交吧，然后所有的节点都commit
     *      }else(没有达到半数){
     *          1，该请求失败，抛出异常
     *      }
     * }
     *
     *
     */

    /**
     * 子问题，如何确定是不是有有效的leader存在
     *
     */

}
