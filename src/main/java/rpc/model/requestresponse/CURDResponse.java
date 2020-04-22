package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/15/2020 15:40
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class CURDResponse extends Response {
    private static final long serialVersionUID = 9090842719326640223L;
    public boolean success;
    public Object value;
}
