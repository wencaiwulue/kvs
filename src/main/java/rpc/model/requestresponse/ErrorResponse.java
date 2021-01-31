package rpc.model.requestresponse;

import lombok.*;

/**
 * @author naison
 * @since 4/12/2020 15:08
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ErrorResponse extends Response {
    private static final long serialVersionUID = 2408416712286509621L;
    private int errorCode;
    private String errorMsg;

    public ErrorResponse(String errorMsg) {
        this.errorMsg = errorMsg;
        this.errorCode = 500;
    }
}
