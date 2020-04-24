package raft.processor;

import db.core.StateMachine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import raft.LogEntry;
import raft.Node;
import rpc.Client;
import rpc.model.requestresponse.*;
import util.BackupUtil;
import util.RetryUtil;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * @author naison
 * @since 4/13/2020 22:38
 */
public class InstallSnapshotRequestProcessor implements Processor {

    private static final Logger log = LogManager.getLogger(InstallSnapshotRequestProcessor.class);

    @Override
    public boolean supports(Request req) {
        return req instanceof InstallSnapshotRequest;
    }

    @Override
    public Response process(Request req, Node node) {
        // copy file, apply
        InstallSnapshotRequest request = (InstallSnapshotRequest) req;
        if (request.fileSize == 0) {
            log.error("this is impossible");
        }

        FileChannel channel = null;
        try {
            channel = FileChannel.open(Path.of(request.filename), StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (IOException e) {
            log.error(e);
        }
        assert channel != null;


        // 应该直接写盘，还是都放在内存中(内存可能会炸)呢?
        int times = (int) Math.ceil(request.fileSize / 4096D);
        int p = 0;
        int offset = 0;
        int length = 1024 * 10 * 10; // 10MB
        do {
            int finalOffset = offset;
            Callable<Response> c = () -> Client.doRequest(request.leader, new DownloadFileRequest(request.filename, finalOffset, length));
            Function<Response, Boolean> f = Objects::isNull;
            DownloadFileResponse response = (DownloadFileResponse) RetryUtil.retryWithResultChecker(c, f, 3);

            if (response != null) {
                try {
                    channel.write(ByteBuffer.wrap(response.bytes));
                    channel.force(true);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                offset += Math.min(response.bytes.length, length);
            } else {
                log.error("what should i do?");
            }
        } while (p++ < times);


        RandomAccessFile file = null;
        try {
            file = new RandomAccessFile(request.filename, "rw");
        } catch (FileNotFoundException e) {
            log.error(e);
        }

        TreeMap<String, Object> treeMap = new TreeMap<>();
        BackupUtil.readFromDisk(treeMap, file);

        for (Object o : treeMap.values()) {
            StateMachine.writeLogToDB(node, (LogEntry) o);
        }

        return new InstallSnapshotResponse(true);
    }
}
