package com.nfsq.yqf.zookeeper.watch;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ConnectWatch implements Watcher {
    private static final String CONNECTION_STRING = "127.0.0.1:2181";
    private static final int SESSION_TIMEOUT = 50000;
    private static ZooKeeper zk;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    @Override
    public void process(WatchedEvent watchedEvent) {
        if(watchedEvent.getState()== Event.KeeperState.SyncConnected){
            System.out.println("######事件通知，有一个客户端已经建立连接"+"path:"+watchedEvent.getPath()+"类型："+watchedEvent.getType());
            if(watchedEvent.getType()== Event.EventType.None){
                System.out.println("####事件通知：状态发生变更"+"path:"+watchedEvent.getPath()+"类型："+watchedEvent.getType());
            }else if(watchedEvent.getType()== Event.EventType.NodeCreated){
                System.out.println("####事件通知：有一个节点被创建"+"path:"+watchedEvent.getPath()+"类型："+watchedEvent.getType());
            }else if(watchedEvent.getType()== Event.EventType.NodeDataChanged){
                System.out.println("####事件通知：有一个节点的数据被修改"+"path:"+watchedEvent.getPath()+"类型："+watchedEvent.getType());
            }else if(watchedEvent.getType()== Event.EventType.NodeDeleted){
                System.out.println("####事件通知：有一个节点的数据被删除"+"path:"+watchedEvent.getPath()+"类型："+watchedEvent.getType());
            }
        }
        countDownLatch.countDown();
    }
    public void connection() throws Exception{
        zk = new ZooKeeper(CONNECTION_STRING,SESSION_TIMEOUT,this);
        countDownLatch.await();
    }
    public void close() throws Exception{
        zk.close();
    }
    public void createNode(String nodeName){
        try{
            String path = "/"+nodeName;
            exsist(path,true);
            zk.create(path,nodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void update(String nodeName,String data) throws Exception{
        String path = "/"+nodeName;
        exsist(path,true);
        zk.setData(path,data.getBytes(),-1);
    }

    public void deleteNode(String nodeName) throws Exception{
        String path = "/"+nodeName;
        exsist(path,true);
        List<String> nodeList = zk.getChildren(path,false);
        for(String node : nodeList){
            zk.delete(path+"/"+node,-1);
        }
        zk.delete(path,-1);
    }

    public boolean exsist(String path,boolean isWatch){
        try{
            zk.exists(path,isWatch);
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        try{
            ConnectWatch connectWatch = new ConnectWatch();
            connectWatch.connection();
            connectWatch.createNode("dubbox");
            connectWatch.update("dubbox","yqf");
            connectWatch.deleteNode("dubbox");
            connectWatch.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
