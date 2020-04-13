package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import raft.LogEntry;
import rpc.model.Request;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SetResponse extends Request {
    private static final long serialVersionUID = -6253521216698393268L;
}
