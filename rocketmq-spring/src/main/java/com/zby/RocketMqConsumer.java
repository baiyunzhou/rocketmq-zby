package com.zby;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.support.ApplicationObjectSupport;

import com.alibaba.fastjson.JSON;
import com.zby.process.MsgProcesser;
import com.zby.support.MqProcesser;

public class RocketMqConsumer extends ApplicationObjectSupport implements SmartLifecycle, InitializingBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqConsumer.class);
	private volatile boolean isRunning = false;
	private String instanceName = "consumer-" + System.currentTimeMillis();
	private String consumerGroup = "default-group";
	private String namesrvAddr = "localhost:9876";
	private String topic = "test-topic";
	private String subExpression = "*";
	private int consumeThreadMin = 1;
	private int consumeThreadMax = 5;
	private DefaultMQPushConsumer defaultMQPushConsumer;
	private Map<String, Pair<MsgProcesser, MqProcesser>> processers;

	public String getInstanceName() {
		return instanceName;
	}

	public void setInstanceName(String instanceName) {
		this.instanceName = instanceName;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getSubExpression() {
		return subExpression;
	}

	public void setSubExpression(String subExpression) {
		this.subExpression = subExpression;
	}

	public int getConsumeThreadMin() {
		return consumeThreadMin;
	}

	public void setConsumeThreadMin(int consumeThreadMin) {
		this.consumeThreadMin = consumeThreadMin;
	}

	public int getConsumeThreadMax() {
		return consumeThreadMax;
	}

	public void setConsumeThreadMax(int consumeThreadMax) {
		this.consumeThreadMax = consumeThreadMax;
	}

	@Override
	public void start() {
		isRunning = true;
		try {
			defaultMQPushConsumer.start();
			LOGGER.info("Consumer:【{}】 start success", instanceName);
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		isRunning = false;
		defaultMQPushConsumer.shutdown();
		LOGGER.info("Consumer:【{}】stop success", instanceName);
	}

	@Override
	public boolean isRunning() {
		return isRunning;
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		callback.run();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		initProcesser();
		initConsumer();
	}

	private void initProcesser() {
		processers = new HashMap<>(8);
		LOGGER.info("Looking for MsgProcesser in application context: 【{}】for consumer:【{}】", getApplicationContext(), instanceName);
		String[] names = getApplicationContext().getBeanNamesForType(MsgProcesser.class);
		for (String name : names) {
			MsgProcesser msgProcesser = getApplicationContext().getBean(name, MsgProcesser.class);
			MqProcesser mqProcesser = msgProcesser.getClass().getAnnotation(MqProcesser.class);
			if (StringUtils.equals(topic, mqProcesser.topic())) {
				String tag = mqProcesser.tag();
				Pair<MsgProcesser, MqProcesser> old = processers.get(tag);
				if (null != old) {
					LOGGER.warn("processer:【{}】 in consumer:【{}】 will be override", old.getRight(), instanceName);
				}
				processers.put(tag, Pair.of(msgProcesser, mqProcesser));
				LOGGER.info("Add processer:【{}】 to consumer:【{}】", mqProcesser, instanceName);
			}
		}
	}

	public void initConsumer() throws MQClientException {
		LOGGER.info("Init consumer:{}", getInstanceName());
		defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup);
		defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);
		defaultMQPushConsumer.setInstanceName(instanceName);
		defaultMQPushConsumer.subscribe(topic, subExpression);
		defaultMQPushConsumer.setConsumeThreadMin(consumeThreadMin);
		defaultMQPushConsumer.setConsumeThreadMax(consumeThreadMax);
		defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		LOGGER.info("Consumer:【{}】,details:【{}】", instanceName, defaultMQPushConsumer.toString());
		defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
			@Override
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
				MessageExt messageExt = msgs.get(0);
				String msgId = messageExt.getMsgId();
				String content = new String(messageExt.getBody());
				LOGGER.info("Consumer:{} received message:{},content:{}", instanceName, messageExt.toString(), content);
				String tag = messageExt.getTags();
				Pair<MsgProcesser, MqProcesser> pair = processers.get(tag);
				if (null == pair) {
					LOGGER.error("No processer for topic:{}tag:{},ignore this message", messageExt.getTopic(), tag);
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
				MsgProcesser msgProcesser = pair.getLeft();
				MqProcesser mqProcesser = pair.getRight();
				try {
					Object object = JSON.parseObject(content, mqProcesser.clazz());
					boolean result = msgProcesser.process(messageExt, object);
					if (result) {
						LOGGER.info("Message:messageid={} process success by processer:{}", msgId, mqProcesser.name());
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					} else {
						LOGGER.info("Message:messageid={} process failed by processer:{}", msgId, mqProcesser.name());
					}
					int maxConsumeTimes = mqProcesser.maxConsumeTimes();
					int reconsumeTimes = messageExt.getReconsumeTimes();
					if (reconsumeTimes + 1 >= maxConsumeTimes) {
						LOGGER.info("Message:messageid={} has retry more than {} times,ignore this message", maxConsumeTimes,
								maxConsumeTimes);
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					}
					LOGGER.info("Message:messageid={} process failed.try later.", msgId);
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;

				} catch (Exception e) {
					LOGGER.error("Message:messageid=" + messageExt.getMsgId() + " process failed", e);
					if (messageExt.getReconsumeTimes() >= mqProcesser.maxConsumeTimes()) {
						LOGGER.error(
								"Message:messageid={} has retry more than " + mqProcesser.maxConsumeTimes() + " times,ignore this message",
								msgId);
						return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					}
					return ConsumeConcurrentlyStatus.RECONSUME_LATER;
				}
			}
		});
		LOGGER.info("Consumer:【{}】 init success", getInstanceName());
	}

}
