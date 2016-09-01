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

package org.apache.dr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.dr.util.OozieActionConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility called by oozie workflow engine post workflow execution.
 */
public class DRPostProcessing extends Configured implements Tool {
    private static final Logger LOG = LoggerFactory.getLogger(DRPostProcessing.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = OozieActionConfigurationHelper.createActionConf();
        ToolRunner.run(conf, new DRPostProcessing(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        LOG.info("In DRPostProcessing");

        return 0;
    }

}
