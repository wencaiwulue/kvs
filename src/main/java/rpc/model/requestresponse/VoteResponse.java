package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import rpc.model.Response;

/**
 * @author naison
 * @since 4/12/2020 14:54
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VoteResponse extends Response {
    int term;
    boolean grant;

    public int getTerm() {
        return term;
    }

    public boolean isGrant() {
        return grant;
    }
}
