package com.nfsq.yqf.zookeeper.controller;

import com.nfsq.yqf.zookeeper.service.ZookeeperService;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

//@RestController
/**
 * 此方法存在缺点
 * 事件监听和本类的主线程是两个线程，如果在创建连接后马上去进行创建节点，有可能会出现事件没有监听到。
 * 此种情况下，需要加并发包（concurrent）中的countDownLacth来处理
 */
public class ZookeeperController {
    @Autowired
    ZookeeperService zookeeperService;
    @RequestMapping("/connect")
    public String connect(){
        zookeeperService.connectZookeeper("127.0.0.1:2181", 50000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("事件已经监听");
            }
        });
        return "zookeeper创建连接成功";
    }
    @RequestMapping("/create")
    public String create(String nodeName) throws Exception{
        zookeeperService.createNode(nodeName);
        return "创建节点成功，节点名称："+nodeName;
    }
    @RequestMapping("/close")
    public String close(){
        zookeeperService.close();
        return "zookeeper关闭成功";
    }
}
