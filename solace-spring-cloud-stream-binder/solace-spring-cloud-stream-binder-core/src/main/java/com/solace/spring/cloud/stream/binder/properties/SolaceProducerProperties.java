package com.solace.spring.cloud.stream.binder.properties;

import java.util.HashMap;
import java.util.Map;

public class SolaceProducerProperties extends SolaceCommonProperties {
	private Map<String,String[]> queueAdditionalSubscriptions = new HashMap<>();

	public Map<String, String[]> getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(Map<String, String[]> queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
	}
}
