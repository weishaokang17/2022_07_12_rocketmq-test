package per.wsk.$01;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class AsyncProducer {


    /**
     * 定义同步消息发送者
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        //新建一个消费者，指定这个消费者属于 pg这个消费者组
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.247.130:9876");

        // 指定发送失败后不进行重试发送 默认2次
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 指定新创建的Topic的Queue数量为2，默认为4
//        producer.setDefaultTopicQueueNums(2);
        //设置发送超时时长是5s，默认3s
        producer.setSendMsgTimeout(5000);

        producer.start();

        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            try {
                Message msg = new Message("myTopicA", "myTag", body);
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } // end-for

        producer.shutdown();
    }

    /**
     * 定义异步消息发送生产者
     * @throws MQClientException
     */
    @Test
    public void test01() throws MQClientException, InterruptedException {
        //新建一个消费者，指定这个消费者属于 pg这个消费者组
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("192.168.247.130:9876");

        // 指定异步发送失败后不进行重试发送 默认2次
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 指定新创建的Topic的Queue数量为2，默认为4
        producer.setDefaultTopicQueueNums(2);
        //设置发送超时时长是5s，默认3s
        producer.setSendMsgTimeout(5000);

        producer.start();

        for (int i = 0; i < 100; i++) {
            byte[] body = ("Hi," + i).getBytes();
            try {
                Message msg = new Message("myTopicC", "myTag", body);

                // 异步发送。指定回调
                producer.send(msg, new SendCallback() {
                    // 当producer接收到MQ发送来的ACK后,才会触发该回调方法的执行
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(sendResult);
                    }
                    @Override
                    public void onException(Throwable e) {
                        e.printStackTrace();
                    }
                });
                msg.setKeys("key- " +i);
                SendResult sendResult = producer.send(msg);
                System.out.println(sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } // end-for

        // sleep一会儿
        // 由于采用的是异步发送，发送者必须等到mq发来的ack（证明连接mq网络是连通的），才发送消息。所以若这里不sleep，直接shutdown，相当于没有
        // 发送消息就关闭了发送者，这时会报错
        TimeUnit.SECONDS.sleep(3);
        producer.shutdown();
    }


    /**
     * 定义单向消息发送生产者
     * @throws RemotingException
     * @throws MQClientException
     * @throws InterruptedException
     */
    @Test
    public void test02() throws RemotingException, MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");
        producer.setNamesrvAddr("rocketmqOS:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            byte[] body = ("Hi," + i).getBytes();
            Message msg = new Message("single", "someTag", body);
            // 单向发送 ,该方法没有返回值，不知道消息是否发送成功，发送出去就不管了
            producer.sendOneway(msg);
        }
        producer.shutdown();
        System.out.println("producer shutdown");
    }


    @Test
    public void test03(){


    }


}
