package com.zby.processer;

import org.apache.rocketmq.common.message.MessageExt;

import com.zby.entity.UserMsg;
import com.zby.process.MsgProcesser;
import com.zby.support.MqProcesser;

@MqProcesser(name = "MyProcesser03", topic = "topic-zby", tag = "zby-tag-3", maxConsumeTimes = 2, clazz = UserMsg.class)
public class MyProcesser03 implements MsgProcesser {

	@Override
	public boolean process(MessageExt messageExt, Object object) {
		UserMsg userMsg = (UserMsg) object;
		System.out.println("MyProcesser01:" + userMsg);
		return false;
	}

}
