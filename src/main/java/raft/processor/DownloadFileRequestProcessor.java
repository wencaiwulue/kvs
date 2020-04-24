package raft.processor;

import raft.Node;
import rpc.model.requestresponse.DownloadFileRequest;
import rpc.model.requestresponse.DownloadFileResponse;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * @author naison
 * @since 4/24/2020 15:54
 */
public class DownloadFileRequestProcessor implements Processor {
    @Override
    public boolean supports(Request req) {
        return req instanceof DownloadFileRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        DownloadFileRequest request = (DownloadFileRequest) req;
        try {
            FileChannel open = FileChannel.open(Path.of(request.filename), StandardOpenOption.READ);
            MappedByteBuffer map = open.map(FileChannel.MapMode.READ_WRITE, request.offset, request.length);
            byte[] array = map.array();
            return new DownloadFileResponse(true, array);
        } catch (IOException e) {
            return new DownloadFileResponse(false, null);
        }
    }
}
