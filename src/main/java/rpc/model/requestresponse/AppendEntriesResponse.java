package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesResponse extends Response {
    private static final long serialVersionUID = -6253521216698393268L;
    int term;
    boolean success = false;
    int lastLogIndex;
}
