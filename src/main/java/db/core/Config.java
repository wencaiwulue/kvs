package db.core;

import com.google.common.collect.Range;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Configuration file, after project finished, will change to read from config.conf
 *
 * @author naison
 * @since 5/3/2020 16:21
 */
public interface Config {

    int PORT = Integer.parseInt(System.getenv("port"));
    Path DB_DIR = Path.of("C:\\Users\\89570\\Documents\\kvs_" + PORT + "\\db");
    Path LOG_DIR = Path.of("C:\\Users\\89570\\Documents\\kvs_" + PORT + "\\log");

    Duration APPEND_RATE = Duration.ofSeconds(1); // append per second
    Duration SNAPSHOT_RATE = Duration.ofMinutes(1); // snapshot per second

    // backup data mode. 0: append, 1: snapshot, 2: append + snapshot
    int BACKUP_MODE = 2;
    int CACHE_SIZE = 1000 * 1000 * 10;
    Range<Integer> CACHE_BACKUP_THRESHOLD = Range.closed((int) (0.2 * CACHE_SIZE), (int) (0.8 * CACHE_SIZE));

    // Integer.MAX_VALUE byte is about 2gb disk file
    int MAX_FILE_SIZE = Integer.MAX_VALUE;

    // heartbeat per 20 millisecond, the last and this heart beat difference is 20ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    Duration HEARTBEAT_RATE = Duration.ofMillis(200);
    // elect per 400 millisecond if not delay, the last and this heart beat difference is 400ms, also means if one node lastHeartBeat + heartBeatRate < currentNanoTime, leader dead. should elect leader
    Duration ELECT_RATE = Duration.ofMillis(800);

}
