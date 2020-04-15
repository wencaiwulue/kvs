package rpc.model.requestresponse;

import lombok.Data;
import lombok.NoArgsConstructor;
import raft.enums.CURDOperation;

/**
 * @author naison
 * @since 4/15/2020 15:40
 */
@Data
@NoArgsConstructor
public class CURDKVRequest extends Request{
    private static final long serialVersionUID = -6395433429049464912L;
    CURDOperation operation;
    String key;
    Object value;
}
