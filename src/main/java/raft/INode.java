package raft;

import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

public interface INode extends Runnable {

    Response handle(Request request);

}
