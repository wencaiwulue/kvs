package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/14/2020 10:52
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotResponse extends Response {
    private static final long serialVersionUID = 4562785019544269943L;
    public boolean success;
}
