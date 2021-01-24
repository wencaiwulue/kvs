package rpc.model.requestresponse;

import lombok.Getter;
import lombok.Setter;
import util.IdUtil;

import java.io.Serializable;

/**
 * @author naison
 * @since 4/12/2020 21:07
 */
@Getter
@Setter
public abstract class Request implements Serializable {
    private static final long serialVersionUID = 988750245807348185L;
    private int requestId;

    public Request() {
        this.requestId = IdUtil.get();
    }
}
