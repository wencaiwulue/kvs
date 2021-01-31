package raft.processor;

import db.core.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.LogEntry;
import raft.Node;
import rpc.model.requestresponse.*;
import rpc.netty.RpcClient;
import util.BackupUtil;
import util.RetryUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * @author naison
 * @since 4/13/2020 22:38
 */
public class InstallSnapshotRequestProcessor implements Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstallSnapshotRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof InstallSnapshotRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        // copy file, apply
        InstallSnapshotRequest request = (InstallSnapshotRequest) req;
        if (request.getFileSize() == 0) {
            LOGGER.error("this is impossible");
        }

        FileChannel fileChannel = null;
        try {
            fileChannel = FileChannel.open(Path.of(request.getFilename()), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        assert fileChannel != null;


        // 应该直接写盘，还是都放在内存中(内存可能会炸)呢?
        int times = (int) Math.ceil(request.getFileSize() / 4096D);
        int p = 0;
        int offset = 0;
        int length = 1024 * 10 * 10; // 每次下载10MB,这里如果要多线程下载，相应的连接也需要改
        do {
            int finalOffset = offset;
            Callable<Response> c = () -> RpcClient.doRequest(request.getLeader(), new DownloadFileRequest(request.getFilename(), finalOffset, length));
            Function<Response, Boolean> f = Objects::isNull;
            DownloadFileResponse response = (DownloadFileResponse) RetryUtil.retryWithResultChecker(c, f, 3);

            if (response != null) {
                try {
                    fileChannel.write(ByteBuffer.wrap(response.bytes));// 写入磁盘
                    fileChannel.force(true);
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                }
                offset += Math.min(response.bytes.length, length);
            } else {
                LOGGER.error("what should i do?");
            }
        } while (p++ < times); // 下载文件

        List<Object> list = new LinkedList<>();
        BackupUtil.readFromDisk(list, Path.of(request.getFilename()).toFile());

        for (Object o : list) {
            StateMachine.writeLogToDB(node, (LogEntry) o);
        }

        return new InstallSnapshotResponse(true);
    }
}
