package rpc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

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
public class Response implements Serializable {
    private int code = 200;// 默认是ok的
    private int type;
    private Map<String, Object> body;
}
