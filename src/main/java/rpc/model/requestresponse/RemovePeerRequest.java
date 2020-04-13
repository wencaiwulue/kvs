package rpc.model.requestresponse;

import lombok.Data;

import java.net.InetSocketAddress;

/**
 * @author naison
 * @since 4/12/2020 15:13
 */
@Data
public class RemovePeerRequest extends Request {
    private static final long serialVersionUID = 369763071864425931L;
    InetSocketAddress peer;
}
