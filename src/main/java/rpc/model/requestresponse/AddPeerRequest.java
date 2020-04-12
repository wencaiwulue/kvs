package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import rpc.model.Request;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 15:12
 */
@Data
@AllArgsConstructor
public class AddPeerRequest extends Request {
    InetSocketAddress peer;

    public InetSocketAddress getPeer() {
        return peer;
    }
}
