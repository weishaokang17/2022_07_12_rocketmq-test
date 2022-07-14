package per.wsk.$02;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DelayProducer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("pe");
        producer.setNamesrvAddr("192.168.247.130:9876");
        producer.start();

        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("MyTopicF", "someTag", body);
            // 指定消息延迟等级为3级，即延迟10s
            msg.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(msg);
            // 输出消息被发送的时间
            System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
            System.out.println(" ," + sendResult);
        }
        producer.shutdown();
    }

}
