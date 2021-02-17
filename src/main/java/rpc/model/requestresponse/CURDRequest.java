package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/15/2020 15:40
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CURDRequest extends Request {
    private static final long serialVersionUID = -6395433429049464912L;
    private CURDOperation operation;
    private String[] key;
    private Object[] value;
}
