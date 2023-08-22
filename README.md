## Introduction

**Distribute key-value system, Base on Raft protocol and in memory storage, still developing**

[Reference document](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)
[Reference document](https://raft.github.io)
[Reference document](http://thesecretlivesofdata.com/raft)

## Function

### Already finished functions

- Election function
- Add/Remove cluster node dynamically

### Still developing functions

- Log synchronize
- Backup data

## How to bootstrap this project?

- Clone this project to your idea
- Build and run
    ```shell
    mvn clean package
    ```
  then start the first node:
    ```shell
    port=8000 java -jar target/kvs.jar
    ```
  start the second node:
    ```shell
    port=8001 java -jar target/kvs.jar
    ```
  start the third node:
    ```shell
    port=8002 java -jar target/kvs.jar
    ```

## How to verify the correctness

- After elected, shutdown the follower node, cluster will work nominally
- After elected, shutdown the leader node, cluster will reelect successfully

### Step

- Bootstrap node1, node2 and node3 manually
- Find the test case class AppTest.java
    - Run method addPeer1And2()
      notify node1 and node2 each other
    - Run method addPeer3()
      notify node2 to add a peer node3, so node2 will be leader theoretically
- Congratulations, you already learned how to add a peer to a cluster
    - Run method removePeer1()
      remove peer the cluster leader will notify the followers to remove this node, and node3 will power off atomically,
      at the same time, the cluster keep working properly
- Enjoy, you can try to edit the nodes which one you want to add and remove dynamically!
  
  
  


