package com.baidu.palo.catalog;

import com.baidu.palo.catalog.Table.TableType;
import com.baidu.palo.common.ClientPool;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.util.Daemon;
import com.baidu.palo.system.Backend;
import com.baidu.palo.thrift.BackendService;
import com.baidu.palo.thrift.TNetworkAddress;
import com.baidu.palo.thrift.TTabletStat;
import com.baidu.palo.thrift.TTabletStatResult;

import com.google.common.collect.ImmutableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class TabletStatMgr extends Daemon {
    private static final Logger LOG = LogManager.getLogger(TabletStatMgr.class);

    private AtomicBoolean isStart = new AtomicBoolean(false);

    public TabletStatMgr() {
        super("tablet stat mgr", Config.tablet_stat_update_interval_second * 1000);
    }

    @Override
    public synchronized void start() {
        if (isStart.compareAndSet(false, true)) {
            super.start();
        }
    }

    @Override
    protected void runOneCycle() {
        // We should wait Frontend finished replaying logs, then begin to get tablet status
        while (!Catalog.getInstance().canRead()) {
            LOG.info("Frontend's canRead flag is false, waiting...");
            try {
                // sleep here, not return. because if we return, we have to wait until next round, which may
                // take a long time(default is tablet_stat_update_interval_second: 5 min)
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOG.info("get interrupted exception when sleep: ", e);
                continue;
            }
        }

        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();

        long start = System.currentTimeMillis();
        for (Backend backend : backends.values()) {
            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TTabletStatResult result = client.get_tablet_stat();

                LOG.info("get tablet stat from backend: {}, num: {}", backend.getId(), result.getTablets_statsSize());
                // LOG.debug("get tablet stat from backend: {}, stat: {}", backend.getId(), result.getTablets_stats());
                updateTabletStat(backend.getId(), result);

                ok = true;
            } catch (Exception e) {
                LOG.warn("task exec error. backend[{}]", backend.getId(), e);
            } finally {
                if (ok) {
                    ClientPool.backendPool.returnObject(address, client);
                } else {
                    ClientPool.backendPool.invalidateObject(address, client);
                }
            }
        }
        LOG.info("finished to get tablet stat of all backends. cost: {} ms",
                 (System.currentTimeMillis() - start));

        // after update replica in all backends, update index row num
        start = System.currentTimeMillis();
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Database db = Catalog.getCurrentCatalog().getDb(dbId);
            if (db == null) {
                continue;
            }
            db.writeLock();
            try {
                for (Table table : db.getTables()) {
                    if (table.getType() != TableType.OLAP) {
                        continue;
                    }

                    OlapTable olapTable = (OlapTable) table;
                    for (Partition partition : olapTable.getPartitions()) {
                        long version = partition.getCommittedVersion();
                        long versionHash = partition.getCommittedVersionHash();
                        for (MaterializedIndex index : partition.getMaterializedIndices()) {
                            long indexRowCount = 0L;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletRowCount = 0L;
                                for (Replica replica : tablet.getReplicas()) {
                                    if (replica.checkVersionCatchUp(version, versionHash)
                                            && replica.getRowCount() > tabletRowCount) {
                                        tabletRowCount = replica.getRowCount();
                                    }
                                }
                                indexRowCount += tabletRowCount;
                            } // end for tablets
                            index.setRowCount(indexRowCount);
                        } // end for indices
                    } // end for partitions
                    LOG.info("finished to set row num for table: {} in database: {}",
                             table.getName(), db.getFullName());
                }
            } finally {
                db.writeUnlock();
            }
        }
        LOG.info("finished to update index row num of all databases. cost: {} ms",
                 (System.currentTimeMillis() - start));
    }

    private void updateTabletStat(Long beId, TTabletStatResult result) {
        TabletInvertedIndex invertedIndex = Catalog.getCurrentInvertedIndex();

        for (Map.Entry<Long, TTabletStat> entry : result.getTablets_stats().entrySet()) {
            Replica replica = invertedIndex.getReplica(entry.getKey(), beId);
            if (replica == null) {
                // replica may be deleted from catalog
                continue;
            }
            // TODO(cmy) no db lock protected. I think it is ok even we get wrong row num
            replica.updateStat(entry.getValue().getData_size(), entry.getValue().getRow_num());
        }
    }
}
