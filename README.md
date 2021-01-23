## Introduction

**Distribute key-value system, Base on Raft protocol and in memory storage, still developing**

[Reference document](https://github.com/maemual/raft-zh_cn/blob/master/raft-zh_cn.md)

## Functions

### Already finished functions

- Election function

### Still developing functions

- Add/Remove node to cluster dynamically
- Log synchronize
- Backup data

## How to bootstrap this project?

- Clone this project to your idea
- Run the main class KvsApplication
- Edit configurations, add environment: port=8001
  ![img.png](img.png)
- Copy configuration, rename and add environment: port=8002
  ![img_1.png](img_1.png)
- Copy configuration, rename and add environment: port=8003
  ![img_2.png](img_2.png)
- Run this three configurations
  ![img_3.png](img_3.png)
- Open the console, you can see the log
  ![img_4.png](img_4.png)
  ![img_5.png](img_5.png)

## How to verify the correctness
- After elected, shutdown the follower node, cluster will work nominally
- After elected, shutdown the leader node, cluster will reelect successfully

