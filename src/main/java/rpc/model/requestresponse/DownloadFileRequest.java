package rpc.model.requestresponse;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author naison
 * @since 4/22/2020 17:11
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DownloadFileRequest extends Request {
    private static final long serialVersionUID = -9034560465445163233L;
    public String filename;
    public int offset;
    public int length;
}
