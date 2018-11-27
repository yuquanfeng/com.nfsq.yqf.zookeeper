package com.nfsq.yqf.zookeeper.zookeeper;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperUtil implements Watcher {
    private ZooKeeper zk;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void process(WatchedEvent watchedEvent) {

        if (watchedEvent.getState()==Event.KeeperState.SyncConnected){
            System.out.println("已经监听到一个新的节点创建，节点名称:"+watchedEvent.getPath()+" 节点类型："+watchedEvent.getType());
            countDownLatch.countDown();
        }
    }

    public void connect(String connectString,int sessionTimeOut) throws Exception{
        System.out.println("开始连接zookeeper");
        zk = new ZooKeeper(connectString,sessionTimeOut,this);
        System.out.println("zookeeper连接完成");
        countDownLatch.await();
        System.out.println("当前count："+countDownLatch.getCount());
    }

    public void createNode(String nodeName) throws Exception{
        zk.create("/"+nodeName,nodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建持久节点成功，节点名称"+nodeName);
    }

    /**
     * 得到父节点下的子节点
     * @param fatherNode
     * @throws Exception
     */
    public void getChileNode(String fatherNode) throws Exception{
        String path = "/"+fatherNode;
        List<String> nodeList = zk.getChildren(path,false);
        if(nodeList.size()==0){
            System.out.println("当前父节点下没有子节点");
        }else{
            nodeList.forEach(node->System.out.println("子节点"+node));
        }
    }

    /**
     * 删除节点
     * @param fatherNode
     * @throws Exception
     */
    public void deleteNode(String fatherNode) throws Exception{
        String path = "/"+fatherNode;
        List<String> nodeList = zk.getChildren(path,false);
        try{
            for(String node:nodeList){
                System.out.println("子节点名字："+node);
                zk.delete(path+"/"+node,-1);
            }
            zk.delete(path,-1);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void close() throws Exception{
        zk.close();
        System.out.println("zookeeper关闭完成");
    }

    public static void main(String[] args) throws Exception{
        ZookeeperUtil zkUtil = new ZookeeperUtil();
        zkUtil.connect("127.0.0.1:2181",50000);
        //zkUtil.createNode("dubbox/dubbo");
        //zkUtil.getChileNode("dubbox");
        zkUtil.deleteNode("dubbox");
        zkUtil.close();
    }
}

