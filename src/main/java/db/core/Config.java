package db.core;

import com.google.common.collect.Range;
import util.ParseUtil;

import java.nio.file.Path;
import java.time.Duration;

/**
 * configuration file, after project finished, will change to read from config.conf
 *
 * @author naison
 * @since 5/3/2020 16:21
 */
public interface Config {

    int PORT = Math.max(8000, ParseUtil.parseInt(System.getProperty("port")));
    Path DB_DIR = Path.of("C:\\Users\\89570\\Documents\\kvs_" + PORT + "\\db");
    Path LOG_DIR = Path.of("C:\\Users\\89570\\Documents\\kvs_" + PORT + "\\log");

    Duration APPEND_RATE = Duration.ofSeconds(1); // append per second
    Duration SNAPSHOT_RATE = Duration.ofMinutes(1); // snapshot per second

    int BACKUP_MODE = 2;// 备份方式为增量还是快照，或者是混合模式, 0--append, 1--snapshot, 2--append+snapshot
    int CACHE_SIZE = 1000 * 1000 * 10;// 缓冲区大小
    Range<Integer> CACHE_BACKUP_THRESHOLD = Range.closed((int) (0.2 * CACHE_SIZE), (int) (0.8 * CACHE_SIZE)); // 如果超过这个阈值，就启动备份写入流程
    // 低于这个值就停止写入，因为管道是不停写入的，所以基本不会出现管道为空的情况

    int MAX_FILE_SIZE = Integer.MAX_VALUE;// 大概文件大小为2gb, 不能超过int的最大值

    Duration HEARTBEAT_RATE = Duration.ofMillis(20);// heartbeat per 20 millisecond, the last and this heart beat difference is 20ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    Duration ELECT_RATE = Duration.ofMillis(400); // elect per 400 millisecond if not delay, the last and this heart beat difference is 400ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader

}
