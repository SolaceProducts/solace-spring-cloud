package com.solace.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;

class SolaceConsumerDestination implements ConsumerDestination {
	private String queueName;

	SolaceConsumerDestination(String queueName) {
		this.queueName = queueName;
	}

	@Override
	public String getName() {
		return queueName;
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("SolaceConsumerDestination{");
		sb.append("queueName='").append(queueName).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
