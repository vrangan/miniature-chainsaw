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
package org.miniature.chainsaw.snapshot.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang3.StringUtils;
import org.miniature.chainsaw.exceptions.DRException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCpOptions;

import java.io.IOException;
import java.util.List;

/**
 * Utility to set DistCp options.
 */
public final class DistCpOptionsUtil {
    private static final String TDE_ENCRYPTION_ENABLED = "tdeEncryptionEnabled";

    private DistCpOptionsUtil() {}

    public static DistCpOptions getDistCpOptions(CommandLine cmd,
                                                 List<Path> sourcePaths,
                                                 Path targetPath) throws DRException, IOException {
        DistCpOptions distcpOptions = new DistCpOptions(sourcePaths, targetPath);
        distcpOptions.setBlocking(true);

        distcpOptions.setMaxMaps(Integer.parseInt(cmd.getOptionValue("maxMaps")));
        distcpOptions.setMapBandwidth(Integer.parseInt(cmd.getOptionValue("mapBandwidth")));

        String tdeEncryptionEnabled = cmd.getOptionValue(TDE_ENCRYPTION_ENABLED);
        if (StringUtils.isNotBlank(tdeEncryptionEnabled)
                && tdeEncryptionEnabled.equalsIgnoreCase(Boolean.TRUE.toString())) {
            distcpOptions.setSyncFolder(true);
            distcpOptions.setSkipCRC(true);
        } else {
            String skipChecksum = cmd.getOptionValue(DRDistCpOptions.DISTCP_OPTION_SKIP_CHECKSUM.getName());
            if (StringUtils.isNotEmpty(skipChecksum)) {
                distcpOptions.setSkipCRC(Boolean.parseBoolean(skipChecksum));
            }
        }

        // Settings needed for Snapshot distCp.
        distcpOptions.setSyncFolder(true);

        String ignoreErrors = cmd.getOptionValue(DRDistCpOptions.DISTCP_OPTION_IGNORE_ERRORS.getName());
        if (StringUtils.isNotBlank(ignoreErrors)) {
            distcpOptions.setIgnoreFailures(Boolean.parseBoolean(ignoreErrors));
        }

        String preserveBlockSize = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_BLOCK_SIZE.getName());
        if (StringUtils.isNotBlank(preserveBlockSize) && Boolean.parseBoolean(preserveBlockSize)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);
        }

        String preserveReplicationCount = cmd.getOptionValue(DRDistCpOptions
                .DISTCP_OPTION_PRESERVE_REPLICATION_NUMBER.getName());
        if (StringUtils.isNotBlank(preserveReplicationCount) && Boolean.parseBoolean(preserveReplicationCount)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.REPLICATION);
        }

        String preservePermission = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_PERMISSIONS.getName());
        if (StringUtils.isNotBlank(preservePermission) && Boolean.parseBoolean(preservePermission)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.PERMISSION);
        }

        String preserveUser = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_USER.getName());
        if (StringUtils.isNotBlank(preserveUser) && Boolean.parseBoolean(preserveUser)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.USER);
        }

        String preserveGroup = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_GROUP.getName());
        if (StringUtils.isNotBlank(preserveGroup) && Boolean.parseBoolean(preserveGroup)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.GROUP);
        }

        String preserveChecksumType = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_CHECKSUM_TYPE.getName());
        if (StringUtils.isNotBlank(preserveChecksumType) && Boolean.parseBoolean(preserveChecksumType)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.CHECKSUMTYPE);
        }

        String preserveAcl = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_ACL.getName());
        if (StringUtils.isNotBlank(preserveAcl) && Boolean.parseBoolean(preserveAcl)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.ACL);
        }

        String preserveXattr = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_XATTR.getName());
        if (StringUtils.isNotBlank(preserveXattr) && Boolean.parseBoolean(preserveXattr)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.XATTR);
        }

        String preserveTimes = cmd.getOptionValue(
                DRDistCpOptions.DISTCP_OPTION_PRESERVE_TIMES.getName());
        if (StringUtils.isNotBlank(preserveTimes) && Boolean.parseBoolean(preserveTimes)) {
            distcpOptions.preserve(DistCpOptions.FileAttribute.TIMES);
        }

        return distcpOptions;
    }
}
