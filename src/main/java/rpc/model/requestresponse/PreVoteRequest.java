package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * todo 网络分区产生的脑裂
 *
 * @author naison
 * @since 5/1/2020 13:58
 */
@Getter
@Setter
@NoArgsConstructor
public class PreVoteRequest extends Request {
    private static final long serialVersionUID = 1226179814612301377L;
}
