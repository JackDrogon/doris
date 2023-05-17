// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.binlog;

import org.apache.doris.common.Pair;
import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BinlogManager {
    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private ReentrantReadWriteLock lock;
    private Map<Long, DBBinlog> dbBinlogMap;
    // Pair(commitSeq, timestamp), used for gc
    private List<Pair<Long, Long>> timestamps;

    public BinlogManager() {
        lock = new ReentrantReadWriteLock();
        dbBinlogMap = Maps.newHashMap();
        timestamps = new ArrayList<Pair<Long, Long>>();
    }

    private void addBinlog(long dbId, List<Long> tableIds, TBinlog binlog) {
        LOG.info("add binlog. dbId: {}, tableIds: {}, binlog: {}", dbId, tableIds, binlog);

        DBBinlog dbBinlog;
        lock.writeLock().lock();
        try {
            dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                dbBinlog = new DBBinlog(dbId);
                dbBinlogMap.put(dbId, dbBinlog);
            }
            timestamps.add(Pair.of(binlog.getCommitSeq(), binlog.getTimestamp()));
        } finally {
            lock.writeLock().unlock();
        }

        dbBinlog.addBinlog(tableIds, binlog);
    }

    private void addBinlog(long dbId, List<Long> tableIds, long commitSeq, long timestamp, TBinlogType type, String data) {
        TBinlog binlog = new TBinlog(commitSeq, timestamp, type, data);
        addBinlog(dbId, tableIds, binlog);
    }

    public void addUpsertRecord(UpsertRecord upsertRecord) {
        LOG.info("add upsert record. upsertRecord: {}", upsertRecord);

        long dbId = upsertRecord.getDbId();
        List<Long> tableIds = upsertRecord.getAllReleatedTableIds();
        long commitSeq = upsertRecord.getCommitSeq();
        long timestamp = upsertRecord.getTimestamp();
        TBinlogType type = TBinlogType.UPSERT;
        String data = upsertRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data);
    }

    public void addAddPartitionRecord(AddPartitionRecord addPartitionRecord) {
        LOG.info("add partition record. partitionRecord: {}", addPartitionRecord);

        long dbId = addPartitionRecord.getDbId();
        List<Long> tableIds = new ArrayList<Long>();
        tableIds.add(addPartitionRecord.getTableId());
        long commitSeq = addPartitionRecord.getCommitSeq();
        long timestamp = -1;
        TBinlogType type = TBinlogType.ADD_PARTITION;
        String data = addPartitionRecord.toJson();

        addBinlog(dbId, tableIds, commitSeq, timestamp, type, data);
    }

    // get binlog by dbId, return first binlog.version > version
    public TBinlog getBinlog(long dbId, long tableId, long commitSeq) {
        LOG.info("get binlog. dbId: {}, tableId: {}, commitSeq: {}", dbId, tableId, commitSeq);
        lock.readLock().lock();
        try {
            DBBinlog dbBinlog = dbBinlogMap.get(dbId);
            if (dbBinlog == null) {
                LOG.warn("dbBinlog not found. dbId: {}", dbId);
                return null;
            }

            return dbBinlog.getBinlog(tableId, commitSeq);
        } finally {
            lock.readLock().unlock();
        }
    }

    // gc binlog, remove all binlog timestamp < minTimestamp
    // TODO(Drogon): get minCommitSeq from timestamps
    public void gc(long minTimestamp) {
        // lock.writeLock().lock();
        // for (Pair<Long, Long> pair : timestamps) {
        //     if (pair.first > version) {
        //         break;
        //     }
        //     timestamps.remove(pair);
        // }
        // lock.writeLock().unlock();
    }
}
