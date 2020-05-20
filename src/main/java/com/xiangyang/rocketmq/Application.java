package com.xiangyang.rocketmq;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionState;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@ComponentScan(basePackages = {"com.xiangyang.aa"})
@RestController
public class Application {
	@Resource
	private RocketMQTemplate rocketMQTemplate;
	@Value("spring-transaction-topic")
	private String springTransTopic;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);

	}

	@GetMapping("hi")
	public String hi(String name) {
		this.testRocketMQTemplateTransaction();
		return "hi " ;
	}

	private  void testRocketMQTemplateTransaction() throws MessagingException {
		String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
		for (int i = 0; i < 10; i++) {
			try {

				Message msg = MessageBuilder.withPayload("rocket的事务消息 " + i).
						setHeader(RocketMQHeaders.TRANSACTION_ID, "KEY_" + i).build();
				System.out.println(rocketMQTemplate);
				SendResult sendResult = rocketMQTemplate.sendMessageInTransaction(
						springTransTopic + ":" + tags[i % tags.length], msg, null);
				System.out.printf("发送的消息体 = %s , sendResult=%s %n",
						msg.getPayload(), sendResult.getSendStatus());

				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	@RocketMQTransactionListener
	class TransactionListenerImpl implements RocketMQLocalTransactionListener {
		private AtomicInteger transactionIndex = new AtomicInteger(0);

		private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<String, Integer>();

		@Override
		public RocketMQLocalTransactionState executeLocalTransaction(Message msg, Object arg) {
			String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
			System.out.printf("本地执行的事务ID, msgTransactionId=%s %n",
					transId);
			int value = transactionIndex.getAndIncrement();
			int status = value % 3;
			status = 0;
			localTrans.put(transId, status);
			if (status == 0) {
				// Return local transaction with success(commit), in this case,
				// this message will not be checked in checkLocalTransaction()
				System.out.printf("    # COMMIT # Simulating msg %s related local transaction exec succeeded! ### %n", msg.getPayload());
				return RocketMQLocalTransactionState.COMMIT;
			}

			if (status == 1) {
				// Return local transaction with failure(rollback) , in this case,
				// this message will not be checked in checkLocalTransaction()
				System.out.printf("    # ROLLBACK # Simulating %s related local transaction exec failed! %n", msg.getPayload());
				return RocketMQLocalTransactionState.ROLLBACK;
			}

			System.out.printf("    # UNKNOW # Simulating %s related local transaction exec UNKNOWN! \n");
			return RocketMQLocalTransactionState.UNKNOWN;
		}

		@Override
		public RocketMQLocalTransactionState checkLocalTransaction(Message msg) {
			String transId = (String) msg.getHeaders().get(RocketMQHeaders.TRANSACTION_ID);
			RocketMQLocalTransactionState retState = RocketMQLocalTransactionState.COMMIT;
			Integer status = localTrans.get(transId);
			if (null != status) {
				switch (status) {
					case 0:
						retState = RocketMQLocalTransactionState.UNKNOWN;
						break;
					case 1:
						retState = RocketMQLocalTransactionState.COMMIT;
						break;
					case 2:
						retState = RocketMQLocalTransactionState.ROLLBACK;
						break;
				}
			}
			System.out.printf("------ !!! checkLocalTransaction is executed once," +
							" msgTransactionId=%s, TransactionState=%s status=%s %n",
					transId, retState, status);
			return retState;
		}
	}

}
