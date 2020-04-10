package raft;

import db.core.DB;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import rpc.Client;
import rpc.model.Request;
import rpc.model.RequestTypeEnum;
import rpc.model.Response;
import util.FSTUtil;
import util.KryoUtil;
import util.ThreadUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
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

    private InetSocketAddress address; // 本机的IP和端口信息
    private List<InetSocketAddress> peerAddress; //其它节点的IP和端口信息

    private DB db;
    private StateEnum state = StateEnum.Follower;// 默认是follower角色
    private long timeout = 150;//150ms -- 300ms randomized  超时时间,选举的时候，如果主节点挂了，则所有的节点开始timeout，然后最先timeout结束的节点变为candidate，
    // 参见竞选，然后发送竞选类型的请求，如果半数以上统一，则广播给所有人，
    // leader回一直发送心跳包，如果timeout后还没有发现心跳包来，就说明leader挂了，需要开始选举

    private AtomicLong l = new AtomicLong(0);// 用来生成提案编号使用

    private volatile long lastHeartBeat;
    private final long heartBeatRate = 150;// the last and this heart beat difference is 150ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader

    private volatile int term = 0;// 第几任leader
    private InetSocketAddress lastVoteFor;// 判断当前选举是否已经投票
    private volatile InetSocketAddress leaderAddr;//leader节点信息，因为所有的数据处理都需要leader来操作。

    public Node(InetSocketAddress address, List<InetSocketAddress> peerAddress) {
        this.address = address;
        this.peerAddress = peerAddress;
        this.db = new DB(String.valueOf(address.getPort()));
    }


    @Override
    public void run() {
        Runnable elect = () -> {
            int i = ThreadLocalRandom.current().nextInt(150);// 0-150ms, 随机一段时间，避免同时选举
            if (leaderAddr == null || System.nanoTime() > lastHeartBeat + heartBeatRate + i) {// 很久没有来自leader的心跳，说明leader卒了，选举开始
                elect();
            }
        };
        ThreadUtil.getSchedulePool().scheduleAtFixedRate(elect, 0, 150, TimeUnit.MILLISECONDS);// 定期检查leader是不是死掉了

        Runnable heartbeats = () -> {
            if (isLeader()) {// 如果自己是主领导，就需要给各个节点发送心跳包
                for (InetSocketAddress address : peerAddress) {
                    if (address.equals(this.address)) continue;// 自己不发

                    Response response = Client.doRequest(address, new Request(HeartBeat, Map.of("leader", this.address, "term", this.term)));
                    log.error("收到从follower的心跳回包.{}", response);
                    if (response != null && response.getCode() == AddNewNode) {// 说明之前的机器死掉了，现在活过来了
                        // todo 传输数据
                        Response res = Client.doRequest(address, new Request(Append, Map.of()));
                    }
                    log.error("已经通知了全球人");
                    break;
                }
            }
        };
        ThreadUtil.getSchedulePool().scheduleAtFixedRate(heartbeats, 0, heartBeatRate, TimeUnit.MILLISECONDS);// 每150ms心跳一下
    }

    private void elect() {
        log.error("开始选举");
        AtomicInteger ai = new AtomicInteger(1);//先给自己投一票
        state = StateEnum.Candidate;// 改变状态为candidate
        for (InetSocketAddress addr : peerAddress) {// todo 这里可以使用callable + future，并行加速处理
            if (addr.equals(address)) continue;
            // todo 这里写消息的时候，可以使用DirectByteBuffer实现零拷贝
            Response response = Client.doRequest(addr, new Request(Vote, Map.of("term", this.term + 1)));
            if (response != null && response.getCode() == 200) {
                log.error("收到从:{}的回包:{}", addr, response);
                ai.addAndGet(1);
            } else {
                log.error("竟然不投票，可能是挂掉了，不理它。远端主机为：{}", addr);
            }
        }
        if (ai.get() > Math.ceil(peerAddress.size() / 2D)) {// 超过半数了，我成功了
            log.error("选举成功，选出leader了{}", this.address);
            this.term += this.term + 1;
            this.leaderAddr = this.address;
            this.state = StateEnum.Leader;
        } else {
            log.error("选举失败");
        }
    }


    private boolean checkAlive(SocketChannel channel) {
        return channel != null && channel.isOpen() && channel.isConnected();
    }

    private boolean isLeader() {
        return leaderAddr != null && leaderAddr.equals(this.address) && state == StateEnum.Leader;
    }

    // todo 可以拆分为服务器之间与服务器和client两种，也就是状态协商和curd
    public void handle(Request request, SocketChannel channel, Selector selector) {
        if (request == null) return;

        switch (request.getType()) {
            case HeartBeat: {
                lastHeartBeat = System.nanoTime();
                InetSocketAddress leaderAddr = (InetSocketAddress) request.getBody().get("leader");
                int term = (int) request.getBody().get("term");
                log.error("已经收到来自leader的心跳包, 其中包含了主节点的信息，{}, term:{}", leaderAddr, term);
                if (term > this.term) {// 说明自己已经out了，需要更新主节点信息
                    this.leaderAddr = leaderAddr;
                    this.state = StateEnum.Follower;
                    this.term = term;
                }
                try {
                    channel.write(ByteBuffer.wrap(KryoUtil.asByteArray(new Response(AddNewNode, 0, null))));// 先构成一次完整的rpc
                    channel.register(selector, SelectionKey.OP_READ);
                } catch (ClosedChannelException e) {
                    log.error("心跳包这里的channel又失效了");
                } catch (IOException e) {
                    log.error("心跳包回复失败。{}", e.getMessage());
                }
                break;
            }
            case AddNewNode: {//todo
                try {
                    // todo 从un-commit中拿去日志，还要从主节点中拿取数据，也就是自己out事件里，世界的变化
                    channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response(AddNewNode, 0, null))));// 先构成一次完整的rpc
                    channel.register(selector, SelectionKey.OP_READ);
                } catch (ClosedChannelException e) {
                    log.error("这里的channel又失效了");
                } catch (IOException e) {
                    log.error("回复失败。{}", e.getMessage());
                }
            }
            case Vote: {
                log.error("收到vote请求，{}", request);
                try {
                    Map<String, Object> body = request.getBody();
                    if (this.lastVoteFor == null && (int) body.getOrDefault("term", -1) > this.term) {// 还没投过票
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response(200, 0, null))));
                        this.lastVoteFor = (InetSocketAddress) channel.getRemoteAddress();
                    } else {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response(100, 0, null))));
                    }
                    channel.register(selector, SelectionKey.OP_READ);
                } catch (ClosedChannelException e) {
                    log.error("vote这里的channel又失效了");
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
                        // 这个是回包
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));// 这里返回给前端
                        channel.register(selector, SelectionKey.OP_READ);
                    } catch (ClosedChannelException e) {
                        log.error("tryAccept1这里的channel又失效了");
                    } catch (IOException e) {
                        log.error("tryAccept1回复失败。{}", e.getMessage());
                    }
                } else {
                    try {
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));
                        channel.register(selector, SelectionKey.OP_READ);
                    } catch (ClosedChannelException e) {
                        log.error("tryAccept2这里的channel又失效了");
                    } catch (IOException e) {
                        log.error("tryAccept2回复失败。{}", e.getMessage());
                    }
                }
                break;
            }
            case doAccept: {
                Optional<Map.Entry<String, Object>> entry = request.getBody().entrySet().stream().findFirst();
                entry.ifPresent(stringObjectEntry -> db.set(stringObjectEntry.getKey(), stringObjectEntry.getValue()));
                try {
                    channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response())));
                    channel.register(selector, SelectionKey.OP_READ);
                } catch (ClosedChannelException e) {
                    log.error("vote失效。{}", e.getMessage());
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
                        channel.write(ByteBuffer.wrap(FSTUtil.getConf().asByteArray(new Response(200, RequestTypeEnum.Append, Map.of(key, 1)))));
                        channel.register(selector, SelectionKey.OP_READ);
                    } catch (ClosedChannelException e) {
                        log.error("vote失效。{}", e.getMessage());
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
