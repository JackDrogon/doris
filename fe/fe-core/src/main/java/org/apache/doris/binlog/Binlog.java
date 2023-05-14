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

public class Binlog {
   private long commit_seq; 
   private string meta;
   private string data;

   public Binlog(long version, string meta, string data) {
       this.commit_seq = version;
       this.meta = meta;
       this.data = data;
   }

   public long getCommitSeq() {
       return commit_seq;
   }

   public string getMeta() {
       return meta;
   }

   public string getData() {
       return data;
   }
}
