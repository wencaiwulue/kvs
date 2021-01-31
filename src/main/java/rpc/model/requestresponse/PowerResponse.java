package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author naison
 * @since 4/16/2020 11:28
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PowerResponse extends Response {
    private static final long serialVersionUID = -4842527193910377010L;
    private boolean success;
}
