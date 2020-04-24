package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @author naison
 * @since 4/22/2020 17:35
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class DownloadFileResponse extends Response {
    private static final long serialVersionUID = -7166579237357118025L;
    public boolean success;
    public byte[] bytes;
}
