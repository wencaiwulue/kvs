package raft;


import lombok.Getter;
import lombok.NoArgsConstructor;
import raft.enums.CURDOperation;

import java.io.Serializable;

/**
 * @author naison
 * @since 3/14/2020 19:06
 */
@Getter
@NoArgsConstructor
public class LogEntry implements Serializable {
    public long index;
    int term;
    CURDOperation operation;
    String key;
    Object value;

    public LogEntry(long index, int term, CURDOperation operation, String key, Object value) {
        this.index = index;
        this.term = term;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }
}

