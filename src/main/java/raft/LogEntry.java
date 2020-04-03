package raft;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author naison
 * @since 3/14/2020 19:06
 */
@Data
@AllArgsConstructor
public class LogEntry {
    int index;
    String key;
    String value;
}

