package RocketMQ;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class Consumer {
	public static void main(String[] args) throws InterruptedException, MQClientException {

		// 声明并初始化一个consumer
		// 需要一个consumer group名字作为构造方法的参数，这里为consumer1
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer1");
		// 同样也要设置NameServer地址
		consumer.setVipChannelEnabled(false);
		consumer.setNamesrvAddr("localhost:9876");
		try {
			consumer.subscribe("demo", "TagA");
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
						ConsumeConcurrentlyContext context) {
					for (MessageExt msg : msgs) {
						System.out.println("Message Received: " + new String(msg.getBody()));

					}
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
		System.out.println("Consumer Started.");
	}
}
