package per.wsk.$01;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

import java.util.List;

public class SomeConsumer {


    public static void main(String[] args) throws MQClientException {
        new SomeConsumer().test01();
    }



    public void test01() throws MQClientException {
        // 定义一个pull消费者
        // DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("cg");
        // 定义一个push消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("cg");

        //指定nameServer
        consumer.setNamesrvAddr("192.168.247.130:9876");
        // 指定从第一条消息开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        //指定消费的topic和tag   tag是通配符，表示消费所有的tag
        consumer.subscribe("myTopicC","*");
        // 指定采用“广播模式”进行消费，默认为“集群模式”
//        consumer.setMessageModel(MessageModel.BROADCASTING);

        //注册消息监听器
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            // 一旦broker中有了其订阅的消息,该方法就会被触发执行， 其返回值为当前consumer消费的状态
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    //逐条消费消息，这里只是测试，项目中真正用到mq中肯定是处理具体的业务
                    System.out.println(msg);
                }
                // 返回消费状态：消费成功
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        // 开启消费者消费
        consumer.start();
        System.out.println("Consumer Started");
    }

}
