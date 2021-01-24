package rpc.model.requestresponse;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author naison
 * @since 3/15/2020 11:28
 */
@Getter
@Setter
public abstract class Response implements Serializable {
    private static final long serialVersionUID = -2641871072732556472L;
    private int requestId;
}
