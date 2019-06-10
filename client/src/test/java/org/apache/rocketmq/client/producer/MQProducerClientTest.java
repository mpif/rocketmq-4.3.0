package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

/**
 * @author:
 * @date: 2019-02-26 14:54:07
 */
public class MQProducerClientTest {

    @Test
    public void sendMsgTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        String producerGroupTemp = "producerGroupName";
        String topic = "myTopicTest";
        /**
         * TODO 设置生产者组有什么用？
         */
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
//        producer.setNamesrvAddr("10.58.84.55:9876");
//        producer.setNamesrvAddr("10.58.84.55:9876;10.58.87.142:9876");
        producer.setNamesrvAddr("192.168.199.171:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
//        Message message = new Message(topic, "This is a very huge message9920!".getBytes());
        Message message = null;

        producer.start();

        System.out.println("producer pid=" + UtilAll.getPid());

        int msgCount = 5000;
        for(int i = 0; i < msgCount; i ++) {
            message = new Message(topic, ("This is a very huge message9920_" + i + "!").getBytes());
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
            Thread.sleep(3 * 1000);
        }

        producer.shutdown();

    }

}
