/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("rf14") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set runtime_filter_mode='GLOBAL'"

    sql 'set exec_mem_limit=21G'
    sql 'SET enable_pipeline_engine = true'
    sql 'set parallel_pipeline_task_num=8'
sql 'set be_number_for_test=3'
    def query = """
    explain physical plan
    select 
        100.00 * sum(case
            when p_type like 'PROMO%'
                then l_extendedprice * (1 - l_discount)
            else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-09-01' + interval '1' month;
    """
    
    
    def getRuntimeFilterCountFromPlan = { plan -> {
        int count = 0
        plan.eachMatch("RF\\d+\\[") {
            ch -> count ++
        }
        return count
    }}

    sql "set enable_runtime_filter_prune=false"
    String plan1 = sql "${query}"
    int count1 = getRuntimeFilterCountFromPlan(plan1)
    sql "set enable_runtime_filter_prune=true"
    String plan2 = sql "${query}"

    log.info("tcph_sf1000 h14 before prune:\n" + plan1)
    log.info("tcph_sf1000 h14 after prune:\n" + plan2)

    int count2 = getRuntimeFilterCountFromPlan(plan2)
    assertEquals(count1, count2)
}
