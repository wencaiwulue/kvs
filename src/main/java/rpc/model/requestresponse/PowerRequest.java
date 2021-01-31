package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author naison
 * @since 4/16/2020 11:28
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PowerRequest extends Request {
    private static final long serialVersionUID = -3361700701914559980L;
    private boolean stopService; // still running, but not provide service
    private boolean powerOff; // power off
}
