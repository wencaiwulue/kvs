package db.config;

import com.google.common.collect.Range;

import java.time.Duration;

/**
 * Configuration file, after project finished, will change to read from config.yaml
 *
 * @author naison
 * @since 5/3/2020 16:21
 */
public interface Config {

    Duration APPEND_RATE = Duration.ofSeconds(1); // append per second
    Duration SNAPSHOT_RATE = Duration.ofMinutes(1); // snapshot per second

    // backup data mode. 0: append, 1: snapshot, 2: append + snapshot
    int BACKUP_MODE = 2;
    int CACHE_SIZE = 1000 * 1000 * 10;
    Range<Integer> CACHE_BACKUP_THRESHOLD = Range.closed((int) (0.2 * CACHE_SIZE), (int) (0.8 * CACHE_SIZE));

    // Integer.MAX_VALUE byte is about 2gb disk file
    int MAX_FILE_SIZE = Integer.MAX_VALUE;

}
