package raft.config;

import java.time.Duration;

public interface Constant {

    // heartbeat per 20 millisecond, the last and this heart beat difference is 20ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    Duration HEARTBEAT_RATE = Duration.ofMillis(200);
    // elect per 400 millisecond if not delay, the last and this heart beat difference is 400ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    Duration ELECT_RATE = Duration.ofMillis(800);
}
