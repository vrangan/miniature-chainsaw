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

package org.apache.dr.snapshot.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.dr.exceptions.DRException;
import org.apache.dr.util.HadoopClientFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * Util class for HDFS snapshot based mirroring.
 */
public final class HdfsSnapshotUtil {
    private static final String EMPTY_KERBEROS_PRINCIPAL = "NA";

    public static final String SNAPSHOT_PREFIX = "falcon-snapshot-";
    public static final String SNAPSHOT_DIR_PREFIX = ".snapshot";

    private HdfsSnapshotUtil() {}

    public static DistributedFileSystem getSourceFileSystem(String sourceStorageUrl,
                                                            String sourceExecuteEndpoint,
                                                            String sourcePrincipal,
                                                            Configuration conf) throws DRException {
        Configuration sourceConf = getConfiguration(conf, sourceStorageUrl,
                sourceExecuteEndpoint, sourcePrincipal);
        return HadoopClientFactory.get().createDistributedProxiedFileSystem(sourceConf);
    }

    public static DistributedFileSystem getTargetFileSystem(String targetStorageUrl,
                                                            String targetExecuteEndpoint,
                                                            String targetPrincipal,
                                                            Configuration conf) throws DRException {
        Configuration targetConf = getConfiguration(conf, targetStorageUrl,
                targetExecuteEndpoint, targetPrincipal);
        return HadoopClientFactory.get().createDistributedProxiedFileSystem(targetConf);
    }

    public static String parseKerberosPrincipal(String principal) {
        if (StringUtils.isEmpty(principal) ||
                principal.equals(EMPTY_KERBEROS_PRINCIPAL)) {
            return null;
        }
        return principal;
    }

    public static Configuration getConfiguration(Configuration conf, String storageUrl,
                                                 String executeEndPoint, String kerberosPrincipal) {
        conf.set(HadoopClientFactory.FS_DEFAULT_NAME_KEY, storageUrl);
        conf.set(HadoopClientFactory.MR_JT_ADDRESS_KEY, executeEndPoint);
        conf.set(HadoopClientFactory.YARN_RM_ADDRESS_KEY, executeEndPoint);
        if (StringUtils.isNotBlank(kerberosPrincipal)) {
            conf.set(HadoopClientFactory.NN_PRINCIPAL, kerberosPrincipal);
        }
        return conf;
    }

}
