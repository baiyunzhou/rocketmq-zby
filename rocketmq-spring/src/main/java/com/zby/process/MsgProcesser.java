package com.zby.process;

import org.apache.rocketmq.common.message.MessageExt;

public interface MsgProcesser {
	boolean process(MessageExt messageExt, Object object);
}
