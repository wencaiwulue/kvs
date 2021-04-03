package raft;

import rpc.model.requestresponse.Response;

public interface INode {

    void start();

    Response handle(Object obj);

    void shutdown();
}
