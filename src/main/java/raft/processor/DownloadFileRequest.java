package raft.processor;

import rpc.model.requestresponse.Request;

/**
 * @author naison
 * @since 4/22/2020 17:11
 */
public class DownloadFileRequest extends Request {
    private static final long serialVersionUID = -9034560465445163233L;
    public String filename;
    public int offset;
    public int length;
    public boolean eof;
}
