package db.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/1/2020 11:07
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ExpireKey implements Comparable {
    private String key;
    private long expire;// System.nanoTime();

    @Override
    public int compareTo(Object o) {
        long a = ((ExpireKey) o).expire;
        return Long.compare(expire, a);
    }
}
