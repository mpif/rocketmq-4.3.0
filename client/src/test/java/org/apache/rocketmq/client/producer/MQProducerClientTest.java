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
        String producerGroupTemp = "myProducerGroupName";
        String topic = "myTopicTest";
        /**
         * TODO 设置生产者组有什么用？
         * 1.标识一类producer
         * 2.可以通过运维工具查询这个发送消息应用下有多个producer实例
         * 3.发送分布式事务消息时，如果producer中途意外宕机，Broker会主动回调Producer Group内的任意一台机器来确认事务状态
         */
        DefaultMQProducer producer = new DefaultMQProducer(producerGroupTemp);
//        producer.setNamesrvAddr("10.58.84.55:9876");
//        producer.setNamesrvAddr("10.58.84.55:9876;10.58.87.142:9876");
        producer.setNamesrvAddr("10.58.196.127:9876");
//        producer.setCompressMsgBodyOverHowmuch(16);
        /**
         * 启用发送延迟故障配置
         */
        producer.setSendLatencyFaultEnable(true);

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
