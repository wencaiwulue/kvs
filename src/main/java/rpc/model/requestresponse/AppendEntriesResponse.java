package rpc.model.requestresponse;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Data
@NoArgsConstructor
public class AppendEntriesResponse extends Request {
    private static final long serialVersionUID = -6253521216698393268L;
}
