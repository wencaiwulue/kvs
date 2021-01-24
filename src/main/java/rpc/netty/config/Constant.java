package rpc.netty.config;

public interface Constant {
    String WEBSOCKET_PATH = "/";
    String WEBSOCKET_PROTOCOL = "diy-protocol";
    // header key for handshaking, to tell server which remote client wants to create a connections
    String LOCALHOST = "localhost";
    String LOCALPORT = "localport";
}
