package rpc.model;

/**
 * @author naison
 * @since 3/14/2020 19:40
 */
public interface RequestTypeEnum {
    // 广播新的leader使用
    int HeartBeat = 1;//心跳包, leader发送给非leader的所有节点

    int AddNewNode = 2;// 添加一台新主机, 新的主机加入，主节点会主动通知，同步数据

    // 存储数据用的
    int Vote = 3;// 投票,一次rpc，投票，回包说没问题，投给你

    int Append = 4;// 追加数据使用, 改朝换代时候,落单的人重新加入集群

    int tryAccept = 5;// 阶段一，主节点说是否可以写入!!!, 然后发送rpc给所有的节点，并返回给client成功 一次rpc

    int doAccept = 6;// 阶段一，主节点说写入!!!, 然后发送rpc给所有的节点，并返回给client成功 一次rpc
}
