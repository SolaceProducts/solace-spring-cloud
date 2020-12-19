package com.solace.spring.cloud.stream.binder.provisioning;

import org.springframework.cloud.stream.provisioning.ConsumerDestination;

import java.util.Collections;
import java.util.List;

public class SolaceConsumerDestination implements ConsumerDestination {
	private String queueName;
	private List<SolaceTopicMatcher> topicMatcher;

	SolaceConsumerDestination(String queueName, List<SolaceTopicMatcher> topicMatcher) {
		this.queueName = queueName;

		Collections.sort(topicMatcher);
		this.topicMatcher = topicMatcher;
	}

	@Override
	public String getName() {
		return queueName;
	}

	public List<SolaceTopicMatcher> getTopicMatcher() {
		return topicMatcher;
	}

	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("SolaceConsumerDestination{");
		sb.append("queueName='").append(queueName).append('\'');
		sb.append('}');
		return sb.toString();
	}
}
