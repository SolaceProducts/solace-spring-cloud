package com.solace.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.provisioning.ProducerDestination;

class SolaceProducerDestination implements ProducerDestination {
	private String topicName;

	SolaceProducerDestination(String topicName) {
		this.topicName = topicName;
	}

	@Override
	public String getName() {
		return topicName;
	}

	@Override
	public String getNameForPartition(int partition) {
		return topicName;
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("SolaceProducerDestination{");
		sb.append("topicName='").append(topicName).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
