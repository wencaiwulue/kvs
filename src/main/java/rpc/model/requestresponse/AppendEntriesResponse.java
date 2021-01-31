package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author naison
 * @since 4/13/2020 14:05
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesResponse extends Response {
    private static final long serialVersionUID = -6253521216698393268L;
    private int term;
    private boolean success = false;
    private int lastLogIndex;
}
