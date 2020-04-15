package raft.enums;

/**
 * @author naison
 * @since 4/15/2020 16:01
 */
public enum CURDOperation {
    get,
    set,
    remove,
    expire,
    mset, // batch operation
    mget,
    mremove
}
