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
public class SetRequest extends Request {
    private static final long serialVersionUID = -2322012843577274410L;
    List<LogEntry> data;
    InetSocketAddress leaderAddr;
}
