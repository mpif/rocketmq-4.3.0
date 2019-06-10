package org.apache.rocketmq.tools.command.topic;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.junit.Test;

/**
 * @author:
 * @date: 2019-02-26 16:20:31
 */
public class MQTopicTest {

    @Test
    public void topicListTest() throws RemotingException, MQClientException, InterruptedException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr("10.75.167.72:9876");
        defaultMQAdminExt.start();

        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        System.out.println(topicList.toJson());

    }

}
