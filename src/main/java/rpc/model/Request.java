package rpc.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * @author naison
 * @since 3/15/2020 11:27
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Request implements Serializable {
    int type;
    Map<String, Object> body;
}
