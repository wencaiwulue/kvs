package raft.processor;

import raft.Node;
import rpc.model.requestresponse.Request;
import rpc.model.requestresponse.Response;

/**
 * @author naison
 * @since 4/12/2020 17:08
 */
public interface Processor {

    boolean supports(Request req);

    Response process(Request req, Node node);
}
