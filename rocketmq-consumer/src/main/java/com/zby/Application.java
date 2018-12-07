package com.zby;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = "com.zby.processer")
public class Application {

	@Bean
	public RocketMqConsumer rocketMqConsumer() {
		RocketMqConsumer rocketMqConsumer = new RocketMqConsumer();
		rocketMqConsumer.setInstanceName("zby");
		rocketMqConsumer.setConsumerGroup("zby-group");
		rocketMqConsumer.setTopic("topic-zby");
		rocketMqConsumer.setSubExpression("*");
		return rocketMqConsumer;
	}

	public static void main(String[] args) throws Exception {
		AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext(Application.class);
		System.in.read();
		applicationContext.close();
	}

}
