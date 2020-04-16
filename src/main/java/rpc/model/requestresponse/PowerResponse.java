package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/16/2020 11:28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PowerResponse extends Response {
    private static final long serialVersionUID = -4842527193910377010L;
    boolean success;
}
