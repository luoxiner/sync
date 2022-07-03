package com.alibaba.nacossync.extension.impl;

import com.alibaba.nacossync.cache.SkyWalkerCacheServices;
import com.alibaba.nacossync.constant.ClusterTypeEnum;
import com.alibaba.nacossync.extension.SyncService;
import com.alibaba.nacossync.extension.annotation.NacosSyncService;
import com.alibaba.nacossync.extension.holder.ZookeeperServerHolder;
import com.alibaba.nacossync.monitor.MetricsManager;
import com.alibaba.nacossync.pojo.model.TaskDO;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    private Map<String, TreeCache> dstTreeCacheMap = new ConcurrentHashMap<>();

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

        TreeCache treeCache = getTreeCache(taskDO.getSourceClusterId(), taskDO.getTaskId());
        Objects.requireNonNull(treeCache).getListenable().addListener((client,event) -> {

        });

        return false;
    }

    @Override
    public boolean needSync(Map<String, String> sourceMetaData) {
        return SyncService.super.needSync(sourceMetaData);
    }

    @Override
    public boolean needDelete(Map<String, String> destMetaData, TaskDO taskDO) {
        return SyncService.super.needDelete(destMetaData, taskDO);
    }

    protected TreeCache getTreeCache(String clusterId,String taskId) {
        return srcTreeCacheMap.computeIfAbsent(taskId, (key) -> {
            try {
                TreeCache treeCache =
                        new TreeCache(zookeeperServerHolder.get(clusterId),
                                DUBBO_ROOT_PATH);
                treeCache.start();
                return treeCache;
            } catch (Exception e) {
                log.error("zookeeper path children cache start failed, taskId:{}", taskId, e);
                return null;
            }
        });
    }

    protected void syncAllDataFromSrcToDstCluster(TaskDO taskDO) {
        CuratorFramework client = zookeeperServerHolder.get(taskDO.getSourceClusterId());

    }
}
