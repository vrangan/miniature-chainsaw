/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.miniature.chainsaw.hive;

/**
 * Test class for DR.
 */
public class DRTest {
    public void testHiveDr(String[] args) {
        String[] testArgs = {
            "-sourceMetastoreUri", "thrift://localhost:9083",
            "-sourceDatabases", "default",
            "-sourceTables", "test",
            "-sourceStagingPath", "/apps/hive/tools/dr",
            "-sourceNN", "hdfs://localhost:8020",
            "-sourceRM", "local",

            "-targetMetastoreUri", "thrift://localhost:9083",
            "-targetStagingPath", "/apps/hive/tools/dr",
            "-targetNN", "hdfs://localhost:8020",
            "-targetRM", "local",

            "-maxEvents", "5",
            "-replicationMaxMaps", "1",
            "-distcpMapBandwidth", "4",
        };
        HiveDRTool.main(testArgs);
    }
}