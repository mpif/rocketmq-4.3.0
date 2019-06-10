/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.*;

public class QueryMsgByUniqueKeySubCommandTest_old {

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());

    QueryMsgByUniqueKeySubCommand cmd = new QueryMsgByUniqueKeySubCommand();

    @Before
    public void before() {

    }


    @Test
    public void testExecuteWithMock() {

        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
//        有点地方是添加下面这个
//        System.setProperty(MixAll.NAMESRV_ADDR_ENV, "127.0.0.1:9876");

            String[] args = new String[]{"-n 127.0.0.1:9876", "-t myTopicTest", "-i 0A4BA748704118B4AAC284DFEEDA0000"};
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, cmd.buildCommandlineOptions(options), new PosixParser());
            cmd.execute(commandLine, options, null);
        } catch (SubCommandException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testExecute() throws SubCommandException {

        QueryMsgByUniqueKeySubCommand queryMsgByUniqueKeySubCommand = new QueryMsgByUniqueKeySubCommand();

        //必须加这句,否则会报如下错误：
        //   Caused by: org.apache.rocketmq.client.exception.MQClientException: CODE: 17  DESC: The topic[myTopicTest] not matched route info
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
//        有点地方是添加下面这个
//        System.setProperty(MixAll.NAMESRV_ADDR_ENV, "127.0.0.1:9876");

        String[] args = new String[]{"-n 127.0.0.1:9876", "-t myTopicTest", "-i 0A4BA748704118B4AAC284DFEEDA0000"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, queryMsgByUniqueKeySubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByUniqueKeySubCommand.execute(commandLine, options, null);

    }

    @After
    public void after() {

    }


}
