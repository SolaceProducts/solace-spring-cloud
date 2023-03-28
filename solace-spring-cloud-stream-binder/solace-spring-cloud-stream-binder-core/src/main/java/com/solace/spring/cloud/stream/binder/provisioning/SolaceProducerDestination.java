package com.solace.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.provisioning.ProducerDestination;

class SolaceProducerDestination implements ProducerDestination {
	private String destinationName;

	SolaceProducerDestination(String destinationName) {
		this.destinationName = destinationName;
	}

	@Override
	public String getName() {
		return destinationName;
	}

	@Override
	public String getNameForPartition(int partition) {
		return destinationName;
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("SolaceProducerDestination{");
		sb.append("destinationName='").append(destinationName).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
