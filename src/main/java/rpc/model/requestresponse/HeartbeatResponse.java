package rpc.model.requestresponse;

import lombok.Data;
import lombok.NoArgsConstructor;
import rpc.model.Response;

/**
 * @author naison
 * @since 4/12/2020 16:53
 */
@Data
@NoArgsConstructor
public class HeartbeatResponse extends Response {
    private static final long serialVersionUID = 255597849623059585L;
}
