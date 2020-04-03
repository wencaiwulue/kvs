package rpc.model;

import lombok.*;

import java.io.Serializable;
import java.util.Map;

/**
 * @author naison
 * @since 3/15/2020 11:28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class Response implements Serializable {
    private int code = 200;// 默认是ok的
    private String message;
    private int type;
    private Map<String, Object> body;
}
