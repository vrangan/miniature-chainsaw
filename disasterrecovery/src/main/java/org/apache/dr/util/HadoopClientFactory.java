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

package org.apache.dr.util;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.dr.exceptions.DRException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * A factory implementation to dole out FileSystem handles based on the logged in user.
 */
public final class HadoopClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(HadoopClientFactory.class);
    private static final HadoopClientFactory INSTANCE = new HadoopClientFactory();

    public static final String MR_JT_ADDRESS_KEY = "mapreduce.jobtracker.address";
    public static final String YARN_RM_ADDRESS_KEY = "yarn.resourcemanager.address";
    public static final String FS_DEFAULT_NAME_KEY = CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;

    /**
     * Constant for the configuration property that indicates the Name node principal.
     */
    public static final String NN_PRINCIPAL = "dfs.namenode.kerberos.principal";

    private HadoopClientFactory() {
    }

    public static HadoopClientFactory get() {
        return INSTANCE;
    }

    /**
     * Return a FileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided proxyUser/group.
     * @throws org.apache.dr.exceptions.DRException
     *          if the filesystem could not be created.
     */
    public FileSystem createProxiedFileSystem(final Configuration conf)
            throws DRException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            return createProxiedFileSystem(new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new DRException("Exception while getting FileSystem for: " + nameNode, e);
        }
    }

    /**
     * Return a DistributedFileSystem created with the authenticated proxy user for the specified conf.
     *
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided proxyUser/group.
     * @throws org.apache.dr.exceptions.DRException
     *          if the filesystem could not be created.
     */
    public DistributedFileSystem createDistributedProxiedFileSystem(final Configuration conf) throws DRException {
        Validate.notNull(conf, "configuration cannot be null");

        String nameNode = getNameNode(conf);
        try {
            return createDistributedFileSystem(CurrentUser.getProxyUGI(null), new URI(nameNode), conf);
        } catch (URISyntaxException e) {
            throw new DRException("Exception while getting Distributed FileSystem for: " + nameNode, e);
        } catch (IOException e) {
            throw new DRException("Exception while getting Distributed FileSystem: ", e);
        }
    }

    private static String getNameNode(Configuration conf) {
        return conf.get(FS_DEFAULT_NAME_KEY);
    }

    /**
     * This method is called from with in a workflow execution context.
     *
     * @param uri uri
     * @return file system handle
     * @throws DRException
     */
    public FileSystem createProxiedFileSystem(final URI uri) throws DRException {
        return createProxiedFileSystem(uri, new Configuration());
    }

    public FileSystem createProxiedFileSystem(final URI uri,
                                              final Configuration conf) throws DRException {
        Validate.notNull(uri, "uri cannot be null");

        try {
            return createFileSystem(CurrentUser.getProxyUGI(null), uri, conf);
        } catch (IOException e) {
            throw new DRException("Exception while getting FileSystem", e);
        }
    }

    /**
     * Return a FileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return FileSystem created with the provided user/group.
     * @throws org.apache.dr.exceptions.DRException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public FileSystem createFileSystem(UserGroupInformation ugi, final URI uri,
                                       final Configuration conf) throws DRException {
        validateInputs(ugi, uri, conf);

        try {
            // prevent falcon impersonating falcon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.info("Creating FS for the login user {}, impersonation not required",
                        proxyUserName);
                return FileSystem.get(uri, conf);
            }

            LOG.info("Creating FS impersonating user {}", proxyUserName);
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                public FileSystem run() throws Exception {
                    return FileSystem.get(uri, conf);
                }
            });
        } catch (InterruptedException ex) {
            throw new DRException("Exception creating FileSystem:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new DRException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    /**
     * Return a DistributedFileSystem created with the provided user for the specified URI.
     *
     * @param ugi user group information
     * @param uri  file system URI.
     * @param conf Configuration with all necessary information to create the FileSystem.
     * @return DistributedFileSystem created with the provided user/group.
     * @throws org.apache.dr.exceptions.DRException
     *          if the filesystem could not be created.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public DistributedFileSystem createDistributedFileSystem(UserGroupInformation ugi, final URI uri,
                                                             final Configuration conf) throws DRException {
        validateInputs(ugi, uri, conf);
        FileSystem returnFs;
        try {
            // prevent falcon impersonating falcon, no need to use doas
            final String proxyUserName = ugi.getShortUserName();
            if (proxyUserName.equals(UserGroupInformation.getLoginUser().getShortUserName())) {
                LOG.info("Creating Distributed FS for the login user {}, impersonation not required",
                        proxyUserName);
                returnFs = DistributedFileSystem.get(uri, conf);
            } else {
                LOG.info("Creating FS impersonating user {}", proxyUserName);
                returnFs = ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                    public FileSystem run() throws Exception {
                        return DistributedFileSystem.get(uri, conf);
                    }
                });
            }

            return (DistributedFileSystem) returnFs;
        } catch (InterruptedException ex) {
            throw new DRException("Exception creating FileSystem:" + ex.getMessage(), ex);
        } catch (IOException ex) {
            throw new DRException("Exception creating FileSystem:" + ex.getMessage(), ex);
        }
    }

    private void validateInputs(UserGroupInformation ugi, final URI uri,
                                final Configuration conf) throws DRException {
        Validate.notNull(ugi, "ugi cannot be null");
        Validate.notNull(conf, "configuration cannot be null");

        try {
            if (UserGroupInformation.isSecurityEnabled()) {
                LOG.debug("Revalidating Auth Token with auth method {}",
                        UserGroupInformation.getLoginUser().getAuthenticationMethod().name());
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
            }
        } catch (IOException ioe) {
            throw new DRException("Exception while getting FileSystem. Unable to check TGT for user "
                    + ugi.getShortUserName(), ioe);
        }

        validateNameNode(uri, conf);
    }

    private void validateNameNode(URI uri, Configuration conf) throws DRException {
        String nameNode = uri.getAuthority();
        if (StringUtils.isBlank(nameNode)) {
            nameNode = getNameNode(conf);
            if (StringUtils.isNotBlank(nameNode)) {
                try {
                    new URI(nameNode).getAuthority();
                } catch (URISyntaxException ex) {
                    throw new DRException("Exception while getting FileSystem", ex);
                }
            }
        }
    }
}
