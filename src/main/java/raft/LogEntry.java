package raft;


import lombok.Data;

/**
 * @author naison
 * @since 3/14/2020 19:06
 */
@Data
public class LogEntry {
    int index;
    int term;
    String key;
    Object value;

    public LogEntry(int index, int term, String key, Object value) {
        this.index = index;
        this.term = term;
        this.key = key;
        this.value = value;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }
}

