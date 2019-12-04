package com.solace.spring.cloud.stream.binder.properties;

import java.util.HashMap;
import java.util.Map;

public class SolaceProducerProperties extends SolaceCommonProperties {
	private Long msgTtl = null;
	private boolean msgInternalDmqEligible = false;
	private Map<String,String[]> queueAdditionalSubscriptions = new HashMap<>();

	public Long getMsgTtl() {
		return msgTtl;
	}

	public void setMsgTtl(Long msgTtl) {
		this.msgTtl = msgTtl;
	}

	public boolean isMsgInternalDmqEligible() {
		return msgInternalDmqEligible;
	}

	public void setMsgInternalDmqEligible(boolean msgInternalDmqEligible) {
		this.msgInternalDmqEligible = msgInternalDmqEligible;
	}

	public Map<String, String[]> getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(Map<String, String[]> queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
	}
}
