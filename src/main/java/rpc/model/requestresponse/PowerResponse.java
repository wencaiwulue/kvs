package rpc.model.requestresponse;

import lombok.Data;

/**
 * @author naison
 * @since 4/16/2020 11:28
 */
@Data
public class PowerResponse extends Response {
    private static final long serialVersionUID = -4842527193910377010L;
    private boolean success;

    public PowerResponse() {
    }

    public PowerResponse(boolean success) {
        this.success = success;
    }

    public boolean isSuccess() {
        return success;
    }
}