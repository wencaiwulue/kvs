package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import rpc.model.Response;

/**
 * @author naison
 * @since 4/12/2020 15:08
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse extends Response {
    private static final long serialVersionUID = 2408416712286509621L;
    int errorCode;
    String errorMsg;
}
