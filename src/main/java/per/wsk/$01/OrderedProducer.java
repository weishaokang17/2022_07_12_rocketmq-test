package per.wsk.$01;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class OrderedProducer {


    /**
     * 顺序消息
     * @param args
     * @throws MQClientException
     */
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("pg");

        producer.setNamesrvAddr("192.168.247.130:9876");
        producer.start();


        for (int i = 0; i < 20; i++) {
            //订单id
            Integer orderId = i;
            for (int j = 1; j < 5; j++) {
                String msg = null;
                switch (j) {
                    case 1:
                        msg = "已付款";
                        break;
                    case 2:
                        msg = "待收货";
                        break;
                    case 3:
                        msg = "待评价";
                        break;
                    case 4:
                        msg = "订单完成";
                        break;
                    default:
                        break;
                }

                byte[] body = ("消息  "+orderId+" " + msg).getBytes();

                Message message = new Message("TopicE", "TagA", body);
                //订单id作为消息的key
                message.setKeys(orderId.toString());

                SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        //以下是使用消息key作为选择器的选择算法
                        String keys = message.getKeys();
                        Integer key = Integer.parseInt(keys);
                        int index = key % list.size();
                        return list.get(index);

                        //以下是使用参数o作为选择器的选择算法
                        /*Integer id = (Integer) o;
                        int index = id % list.size();
                        return list.get(index);*/
                    }
                }, orderId);

                System.out.println(sendResult);
            }
            //以上保证了: 同一个订单，发送的几次mq消息，都在同一个队列中
        }

        producer.shutdown();
    }

}
