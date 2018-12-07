/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import java.util.Date;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import com.alibaba.fastjson.JSON;
import com.zby.entity.UserMsg;

public class Producer {
	public static void main(String[] args) throws MQClientException, InterruptedException {

		DefaultMQProducer producer = new DefaultMQProducer("zby", new RPCHook() {

			@Override
			public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
				System.out.println("doBeforeRequest:");
				System.out.println("\tromoteAddr:" + remoteAddr);
				System.out.println("\tRemotingCommand" + request);
			}

			@Override
			public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
				System.out.println("doAfterResponse:");
				System.out.println("\tromoteAddr:" + remoteAddr);
				System.out.println("\tRemotingCommand" + request);
				System.out.println("\tRemotingCommand" + response);
			}
		});
		System.out.println(producer);
		producer.setNamesrvAddr("localhost:9876");
		producer.start();

		// producer.createTopic(producer.getCreateTopicKey(), "zby", 4);
		// System.out.println("topicKey:" + producer.getCreateTopicKey());

		for (int i = 0; i < 10; i++) {
			try {
				UserMsg userMsg = new UserMsg((long) i, "name" + i, new Date());
				Message msg = new Message("topic-zby", "zby-tag-3", JSON.toJSONString(userMsg).getBytes(RemotingHelper.DEFAULT_CHARSET));
				// messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
				msg.setDelayTimeLevel(4);
				SendResult sendResult = producer.send(msg);
				System.out.printf("%s%n", sendResult);
			} catch (Exception e) {
				e.printStackTrace();
				Thread.sleep(1000);
			}
		}

		/*
		 * Shut down once the producer instance is not longer in use.
		 */
		producer.shutdown();
	}
}
