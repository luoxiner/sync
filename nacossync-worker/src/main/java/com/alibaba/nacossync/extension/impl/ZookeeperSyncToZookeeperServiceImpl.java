package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacossync.util.DubboConstants.DUBBO_ROOT_PATH;

/**
 * @author luoxi
 * this is a zookeeper to zookeeper data sync implementation
 */
@Slf4j
@NacosSyncService(sourceCluster = ClusterTypeEnum.ZK, destinationCluster = ClusterTypeEnum.ZK)
public class ZookeeperSyncToZookeeperServiceImpl implements SyncService {

    @Autowired
    private MetricsManager metricsManager;

    private ZookeeperServerHolder zookeeperServerHolder;

    private SkyWalkerCacheServices skyWalkerCacheServices;

    private List<String> paths2Sync;

    private Map<String, TreeCache> srcTreeCacheMap = new ConcurrentHashMap<>();

    @Autowired
    public ZookeeperSyncToZookeeperServiceImpl(ZookeeperServerHolder zookeeperServerHolder, SkyWalkerCacheServices skyWalkerCacheServices) {
        this.zookeeperServerHolder = zookeeperServerHolder;
        this.skyWalkerCacheServices = skyWalkerCacheServices;
    }

    @Override
    public boolean delete(TaskDO taskDO) {
        return false;
    }

    @Override
    public boolean sync(TaskDO taskDO) {
        if (srcTreeCacheMap.containsKey(taskDO.getTaskId())) {
            return true;
        }

        CuratorFramework sourceClient = zookeeperServerHolder.get(taskDO.getSourceClusterId());
        CuratorFramework dstClient = zookeeperServerHolder.get(taskDO.getDestClusterId());

        try {
            // clear all the corresponding path in the dst cluster first
            if (dstClient.checkExists().forPath(taskDO.getServiceName()) != null) {
                dstClient.delete().deletingChildrenIfNeeded().forPath(taskDO.getServiceName());
            }
            TreeCache treeCache = getAndStartTreeCache(taskDO);
            syncAllDataFromSrcToDstCluster(taskDO, treeCache);
            registerWatchToSrc(taskDO, treeCache);
        } catch (Exception e) {
            log.error("sync from zookeeper to zookeeper error", e);
            return false;
        }
        return true;
    }

    protected TreeCache getAndStartTreeCache(TaskDO taskDO) {
        return srcTreeCacheMap.computeIfAbsent(taskDO.getTaskId(), (key) -> {
            try {
                TreeCache treeCache =
                        new TreeCache(zookeeperServerHolder.get(taskDO.getSourceClusterId()),
                                DUBBO_ROOT_PATH);
                treeCache.start();
                return treeCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskDO.getTaskId(), e);
                return null;
            }
        });
    }

    protected void syncAllDataFromSrcToDstCluster(TaskDO taskDO, TreeCache treeCache) throws Exception {
        CuratorFramework dstClient = zookeeperServerHolder.get(taskDO.getDestClusterId());
        CuratorFramework srcClient = zookeeperServerHolder.get(taskDO.getSourceClusterId());
        Deque<String> nodeQueueToSync = new LinkedList<>();
        nodeQueueToSync.addLast(taskDO.getServiceName());
        while (!nodeQueueToSync.isEmpty()) {
            String currentPath = nodeQueueToSync.removeFirst();
            ChildData currentData = treeCache.getCurrentData(currentPath);
            Map<String, ChildData> currentChildren = treeCache.getCurrentChildren(currentPath);
            if (currentChildren != null) {
                nodeQueueToSync.addAll(currentChildren.keySet());
            }
            createToDstCluster(taskDO,currentData);
        }
    }

    private CreateMode getCreateMode(ChildData data) {
        return data.getStat().getEphemeralOwner() == 0 ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT;
    }

    private void createToDstCluster(TaskDO taskDO,ChildData data) throws Exception {
        CuratorFramework dstClient = zookeeperServerHolder.get(taskDO.getDestClusterId());
        CuratorFramework srcClient = zookeeperServerHolder.get(taskDO.getSourceClusterId());
        List<ACL> acls = srcClient.getACL().forPath(data.getPath());
        dstClient.create()
                .withMode(getCreateMode(data))
                .withACL(acls)
                .forPath(data.getPath(), data.getData());
    }

    private void syncToDstCluster(TaskDO taskDO,ChildData data) {

    }

    private void registerWatchToSrc(TaskDO taskDO, TreeCache treeCache) {
        treeCache.getListenable().addListener((client, event) -> {
            if (needSync(event.getData())) {
                syncToDstCluster(taskDO,event.getData());
            }
        });
    }

    private boolean needSync(ChildData data) {
        return true;
    }
}
