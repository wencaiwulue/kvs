package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author naison
 * @since 4/14/2020 10:52
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class InstallSnapshotResponse extends Response {
    private static final long serialVersionUID = 4562785019544269943L;
    private boolean success;
}
