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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;

public class TableBinlog {
    private long tableId;
    private List<TBinlog> binlogs;

    public TableBinlog(long tableId) {
        this.tableId = tableId;
        binlogs = new ArrayList<TBinlog>();
    }

    public long getTableId() {
        return tableId;
    }

    public void addBinlog(TBinlog binlog) {
        binlogs.add(binlog);
    }

    public TBinlog getBinlog(long commitSeq) {
        // use java upperBound to get binlog
        int index = Collections.binarySearch(binlogs, commitSeq, new Comparator<TBinlog>() {
            @Override
            public int compare(TBinlog o1, TBinlog o2) {
                return Long.compare(o1.getCommitSeq(), o2.getCommitSeq());
            }
        });
        if (index < 0) {
            index = -index - 1;
        }
        if (index >= binlogs.size()) {
            return null;
        }
        return binlogs.get(index);
    }    
}
