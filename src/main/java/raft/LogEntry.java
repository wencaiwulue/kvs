package raft;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import raft.enums.CURDOperation;

import java.io.Serializable;

/**
 * @author naison
 * @since 3/14/2020 19:06
 */
@Getter
@Setter
@NoArgsConstructor
public class LogEntry implements Serializable {
    private long index;
    private int term;
    private  CURDOperation operation;
    private  String key;
    private  Object value;

    public LogEntry(long index, int term, CURDOperation operation, String key, Object value) {
        this.index = index;
        this.term = term;
        this.operation = operation;
        this.key = key;
        this.value = value;
    }
}

