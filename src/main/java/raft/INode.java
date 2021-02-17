package raft;

import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

public interface INode {

    void start();

    Response handle(Request request);

    void shutdown();
}
