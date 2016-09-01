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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CurrentUser {
    /* TODO: Check about authenticated user */
    private static ConcurrentMap<String, UserGroupInformation> userUgiMap =
            new ConcurrentHashMap<>();

    /**
     * Dole out a proxy UGI object for the current authenticated user if authenticated
     * else return current user.
     *
     * @return UGI object
     * @throws java.io.IOException
     */
    public static UserGroupInformation getProxyUGI(final String user) throws IOException {
        return StringUtils.isNotBlank(user)
                ? createProxyUGI(user) : UserGroupInformation.getCurrentUser();
    }

    /**
     * Create a proxy UGI object for the proxy user.
     *
     * @param proxyUser logged in user
     * @return UGI object
     * @throws IOException
     */
    public static UserGroupInformation createProxyUGI(final String proxyUser) throws IOException {
        UserGroupInformation proxyUgi = userUgiMap.get(proxyUser);
        if (proxyUgi == null) {
            // taking care of a race condition, the latest UGI will be discarded
            proxyUgi = UserGroupInformation.createProxyUser(
                    proxyUser, UserGroupInformation.getLoginUser());

            if (StringUtils.isNotBlank(proxyUser)) {
                userUgiMap.putIfAbsent(proxyUser, proxyUgi);
            }
        }

        return proxyUgi;
    }
}
