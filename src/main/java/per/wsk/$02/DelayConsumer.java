package per.wsk.$02;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;
import per.wsk.$01.SomeConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class DelayConsumer {

    public static void main(String[] args) throws MQClientException {
        DelayConsumer consumer = new DelayConsumer();
        consumer.test01();
    }


    public void test01() throws MQClientException {

        // 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("pe");

        //指定nameServer
        consumer.setNamesrvAddr("192.168.247.130:9876");
        // 指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //指定消费的topic和tag   tag是通配符，表示消费所有的tag
        consumer.subscribe("MyTopicF","*");
        // 指定采用“广播模式”进行消费，默认为“集群模式”
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    // 输出消息被消费的时间
                    System.out.print(new SimpleDateFormat("mm:ss").format(new Date()));
                    System.out.println(" ," + msg);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer Started");
    }


}
