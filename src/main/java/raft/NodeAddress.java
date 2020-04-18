package raft;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * @author naison
 * @since 4/18/2020 11:04
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class NodeAddress implements Serializable {
    private static final long serialVersionUID = 3723897904539137708L;
    public boolean alive;
    public InetSocketAddress socketAddress;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeAddress that = (NodeAddress) o;
        return Objects.equals(socketAddress, that.socketAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(socketAddress);
    }
}
