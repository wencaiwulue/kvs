package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/16/2020 11:28
 */
@NoArgsConstructor
@AllArgsConstructor
public class PowerRequest extends Request {
    private static final long serialVersionUID = -3361700701914559980L;
    public boolean stopService; // still running, but not provide service
    public boolean powerOff; // power off
}
