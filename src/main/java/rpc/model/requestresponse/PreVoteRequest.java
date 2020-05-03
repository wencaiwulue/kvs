package rpc.model.requestresponse;

import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * todo 网络分区产生的脑裂
 *
 * @author naison
 * @since 5/1/2020 13:58
 */
@NoArgsConstructor
@Getter
public class PreVoteRequest extends Request {
    private static final long serialVersionUID = 1226179814612301377L;
}
