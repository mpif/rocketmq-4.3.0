package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class QueryMsgByIdSubCommandTest {

    private static QueryMsgByIdSubCommand queryMsgByIdSubCommand;

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;

    @BeforeClass
    public static void init() {

        queryMsgByIdSubCommand = new QueryMsgByIdSubCommand();

    }

    @Test
    public void testExecute() throws SubCommandException {

        //-i 要传offsetMsgId,而不是msgId
        String[] args = new String[]{"-n 127.0.0.1:9876", "-g producerGroupName", "-i 0A4BA74800002A9F00000000000004BC"};
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin ", args, queryMsgByIdSubCommand.buildCommandlineOptions(options), new PosixParser());
        queryMsgByIdSubCommand.execute(commandLine, options, null);

    }

    @Test
    public void queryMsgIdTest() throws MQClientException, RemotingException, InterruptedException, MQBrokerException, UnsupportedEncodingException {

        String namesrvAddr = "10.75.167.72:9876";
        String topic = "myTopicTest";
        String msgId = "0A4BA748704118B4AAC284DFEEDA0000";

//        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("ReSendMsgById");
//        defaultMQProducer.setInstanceName(Long.toString(System.currentTimeMillis()));
//        defaultMQProducer.setNamesrvAddr(namesrvAddr);
//        defaultMQProducer.start();

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr(namesrvAddr);
        defaultMQAdminExt.start();

//        MessageExt msg = defaultMQAdminExt.viewMessage(msgId);
        MessageExt msg = defaultMQAdminExt.viewMessage(topic, msgId);
        String msgBody = new String(msg.getBody(), "UTF-8");
        System.out.println("msgId=" + msg.getMsgId() + ", msgBody=" + msgBody);
        System.out.println("topic=" + msg.getTopic() + ", queueId=" + msg.getQueueId() + ", queueOffset=" + msg.getQueueOffset());

        List<MessageTrack> mtdList = defaultMQAdminExt.messageTrackDetail(msg);
        if (mtdList.isEmpty()) {
            System.out.printf("%n%nWARN: No Consumer");
        } else {
            System.out.printf("%n%n");
            for (MessageTrack mt : mtdList) {
                System.out.printf("%s", mt);
            }
        }

    }

    @AfterClass
    public static void terminate() {
    }


}
