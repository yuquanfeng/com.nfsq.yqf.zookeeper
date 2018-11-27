package com.nfsq.yqf.zookeeper.service;

import org.apache.zookeeper.Watcher;

public interface ZookeeperService {
    void connectZookeeper(String connectString, int sessionTimeOut, Watcher watcher);
    void close();
    void createNode(String nodeName) throws Exception;
}
