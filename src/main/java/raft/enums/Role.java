package raft.enums;

/**
 * @author naison
 * @since 3/14/2020 19:03
 */
public enum Role {
    LEADER,//主节点，只有一个leader，所有的请求都是从这里发起的，也就是如果client链接上任意一条机器，如果这台机器不是主节点,
    //  需要转发到主节点机器上，然后再由主节点处理，如果主节点down了，需要迅速选举
    CANDIDATE,//候选者，临时的状态，leader宕机后，所有的节点都会成为candidate
    FOLLOWER//跟随者，默认的状态，大家都是follower
}
