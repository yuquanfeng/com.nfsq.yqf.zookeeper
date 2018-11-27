package com.nfsq.yqf.zookeeper.service.impl;

import com.nfsq.yqf.zookeeper.service.ZookeeperService;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.stereotype.Service;

@Service
public class ZookeeperServiceImpl implements ZookeeperService {
    private static ZooKeeper zk = null;
    @Override
    /**
     * 代码实现连接zookeeper
     */
    public void connectZookeeper(String connectString, int sessionTimeOut, Watcher watcher) {
        try{
            System.out.println("zookeeper开始建立连接");

            zk = new ZooKeeper(connectString,sessionTimeOut,watcher);

            System.out.println("zookeeper连接成功");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    /**
     * 手动创建节点
     */
    public void createNode(String nodeName) throws Exception{
        String path= "/"+nodeName;
        zk.create(path,path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    }

    @Override
    /**
     * 手动关闭zookeeper
     */
    public void close(){
        try{
            if(zk!=null){
                zk.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
