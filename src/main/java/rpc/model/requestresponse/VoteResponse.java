package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/12/2020 14:54
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class VoteResponse extends Response {
    private static final long serialVersionUID = 6388639175646732562L;
    int term;
    boolean grant;

    public int getTerm() {
        return term;
    }

    public boolean isGrant() {
        return grant;
    }
}
