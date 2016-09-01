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

package org.apache.dr.snapshot.replication;

import org.apache.dr.exceptions.DRException;
import org.apache.dr.snapshot.util.DRDistCpOptions;
import org.apache.dr.snapshot.util.HdfsSnapshotMirrorProperties;
import org.apache.dr.snapshot.util.HdfsSnapshotUtil;
import org.apache.dr.snapshot.util.MiniHdfsClusterUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.apache.commons.cli.CommandLine;

import java.io.File;
import java.nio.file.Files;

/**
 * Hdfs Snapshot replicator unit tests.
 */
public class HdfsSnapshotReplicatorTest {
    private MiniDFSCluster miniDFSCluster;
    private HdfsSnapshotReplicator replicator;
    private CommandLine cmd;
    private DistributedFileSystem miniDfs;
    private File baseDir;
    private String sourceStorageUrl;
    private String targetStorageUrl;
    private final String SOURCEDIRPATH = "/apps/dr/snapshot-replication/sourceDir";
    private final String TARGETDIRPATH = "/apps/dr/snapshot-replication/targetDir";
    private Path sourceDir = new Path(SOURCEDIRPATH);
    private Path targetDir = new Path(TARGETDIRPATH);

    private FsPermission fsPermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private String[] args = {
            "--" + HdfsSnapshotMirrorProperties.MAX_MAPS.getName(), "1",
            "--" + HdfsSnapshotMirrorProperties.MAP_BANDWIDTH_IN_MB.getName(), "100",
            "--" + HdfsSnapshotMirrorProperties.SOURCE_NN.getName(), "hdfs://localhost:54136",
            "--" + HdfsSnapshotMirrorProperties.SOURCE_EXEC_URL.getName(), "localhost:8021",
            "--" + HdfsSnapshotMirrorProperties.TARGET_EXEC_URL.getName(), "localhost:8021",
            "--" + HdfsSnapshotMirrorProperties.TARGET_NN.getName(), "hdfs://localhost:54136",
            "--" + HdfsSnapshotMirrorProperties.SOURCE_SNAPSHOT_DIR.getName(),
            SOURCEDIRPATH,
            "--" + HdfsSnapshotMirrorProperties.TARGET_SNAPSHOT_DIR.getName(),
            TARGETDIRPATH,
            "--" + DRDistCpOptions.DISTCP_OPTION_IGNORE_ERRORS.getName(), "false",
            "--" + DRDistCpOptions.DISTCP_OPTION_PRESERVE_ACL.getName(), "false",
            "--" + HdfsSnapshotMirrorProperties.TDE_ENCRYPTION_ENABLED.getName(), "false",
            "--" + HdfsSnapshotMirrorProperties.SNAPSHOT_JOB_NAME.getName(), "snapshotJobName",
    };

    @BeforeClass
    public void init() throws Exception {
        replicator = new HdfsSnapshotReplicator();
        replicator.setConf(new Configuration());
        cmd = replicator.getCommand(args);

        baseDir = Files.createTempDirectory("test_snapshot-replication").toFile().getAbsoluteFile();
        miniDFSCluster = MiniHdfsClusterUtil.initMiniDfs(MiniHdfsClusterUtil.SNAPSHOT_REPL_TEST_PORT, baseDir);
        miniDfs = miniDFSCluster.getFileSystem();

        sourceStorageUrl = "hdfs://localhost:54136";
        targetStorageUrl = "hdfs://localhost:54136";

        miniDfs.mkdirs(sourceDir, fsPermission);
        miniDfs.mkdirs(targetDir, fsPermission);

        miniDfs.allowSnapshot(sourceDir);
        miniDfs.allowSnapshot(targetDir);
    }

    @Test
    public void replicationTest() throws Exception {
        DistributedFileSystem sourceFs = HdfsSnapshotUtil.getSourceFileSystem("hdfs://localhost:54136", "localhost:8021", "NA",
                new Configuration(replicator.getConf()));
        DistributedFileSystem targetFs = HdfsSnapshotUtil.getTargetFileSystem("hdfs://localhost:54136", "localhost:8021", "NA",
                new Configuration(replicator.getConf()));

        // create dir1, create snapshot, invoke copy, check file in target, create snapshot on target
        Path dir1 = new Path(sourceDir, "dir1");

        miniDfs.mkdir(dir1, fsPermission);
        miniDfs.createSnapshot(sourceDir, "snapshot1");
        replicator.invokeCopy(cmd, sourceStorageUrl, targetStorageUrl, sourceFs, targetFs,
                sourceDir.toString(), targetDir.toString(), "snapshot1");
        miniDfs.createSnapshot(targetDir, "snapshot1");
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir1")));

        // create dir2, create snapshot, invoke copy, check dir in target, create snapshot on target
        Path dir2 = new Path(sourceDir, "dir2");
        miniDfs.mkdir(dir2, fsPermission);
        miniDfs.createSnapshot(sourceDir, "snapshot2");
        replicator.invokeCopy(cmd, sourceStorageUrl, targetStorageUrl, sourceFs, targetFs,
                sourceDir.toString(), targetDir.toString(), "snapshot2");
        miniDfs.createSnapshot(targetDir, "snapshot2");
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir2")));

        // delete dir1, create snapshot, invoke copy, check file not in target
        miniDfs.delete(dir1, true);
        miniDfs.createSnapshot(sourceDir, "snapshot3");
        replicator.invokeCopy(cmd, sourceStorageUrl, targetStorageUrl, sourceFs, targetFs,
                sourceDir.toString(), targetDir.toString(), "snapshot3");
        miniDfs.createSnapshot(targetDir, "snapshot3");
        Assert.assertFalse(miniDfs.exists(new Path(targetDir, "dir1")));
        Assert.assertTrue(miniDfs.exists(new Path(targetDir, "dir2")));
    }

    @Test(dependsOnMethods = "replicationTest",
            expectedExceptions = DRException.class,
            expectedExceptionsMessageRegExp = "Unable to find latest snapshot on targetDir "
                    + TARGETDIRPATH)
    public void removeSnapshotabilityOnTargetTest() throws Exception {
        // remove snapshotability on target, create snapshot on source, invoke copy should fail
        miniDfs.deleteSnapshot(targetDir, "snapshot1");
        miniDfs.deleteSnapshot(targetDir, "snapshot2");
        miniDfs.deleteSnapshot(targetDir, "snapshot3");


        miniDfs.disallowSnapshot(targetDir);
        Path dir1 = new Path(sourceDir, "dir4");
        miniDfs.mkdir(dir1, fsPermission);
        miniDfs.createSnapshot(sourceDir, "snapshot4");
        replicator.invokeCopy(cmd, sourceStorageUrl, targetStorageUrl, miniDfs, miniDfs,
                sourceDir.toString(), targetDir.toString(), "snapshot4");
    }

    @AfterClass
    public void cleanup() throws Exception {
        MiniHdfsClusterUtil.cleanupDfs(miniDFSCluster, baseDir);
    }
}
