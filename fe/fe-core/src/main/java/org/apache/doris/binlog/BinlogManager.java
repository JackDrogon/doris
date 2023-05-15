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

import org.apache.doris.thrift.TBinlog;
import org.apache.doris.thrift.TBinlogType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TBinlogManager {
    private static final Logger LOG = LogManager.getLogger(BinlogManager.class);

    private ReentrantReadWriteLock lock;
    private Map<Long, DBBinlog> dbBinlogMap;

    public TBinlogManager() {
        lock = new ReentrantReadWriteLock();
        dbBinlogMap = Maps.newHashMap();
    }

    private void addBinlog(long dbId, List<Long> tableIds, TBinlog binlog) {
        lock.writeLock().lock();
        DBBinlog dbBinlog = dbBinlogMap.get(dbId);
        if (dbBinlog == null) {
            dbBinlog = new DBBinlog(dbId);
            dbBinlogMap.put(dbId, dbBinlog);
        }
        dbBinlog.addBinlog(tableIds, binlog);
        lock.writeLock().unlock();
    }

    public void addUpsertRecord(UpsertRecord upsertRecord) {
        long dbId = upsertRecord.getDbId();
        List<Long> tableId = upsertRecord.getAllReleatedTableIds();
        long commitSeq = upsertRecord.getCommitSeq();
        TBinlog binlog = new TBinlog(commitSeq, TBinlogType.UPSERT, upsertRecord.toJson())
        addBinlog(dbId, tableId, binlog);
    }

    // get binlog by dbId, return first binlog.version > version
    public TBinlog getBinlog(long dbId, long tableId, long commitSeq) {
        lock.readLock().lock();
        Map<Long, DBBinlog> dbBinlog = dbBinlogMap.get(dbId);
        if (dbBinlog == null) {
            LOG.warn("dbBinlog not found. dbId: {}", dbId);
            return null;
        }

        TBinlog binlog = dbBinlog.getBinlog(tableId, commitSeq);
        lock.readLock().unlock();
    }
}
