package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class FlowsHealthContributor implements CompositeHealthContributor {
	private final Map<String, FlowHealthIndicator> flowHealthContributor = new HashMap<>();

	public void addFlowContributor(String flowId, FlowHealthIndicator flowHealthIndicator) {
		flowHealthContributor.put(flowId, flowHealthIndicator);
	}

	public void removeFlowContributor(String flowId) {
		flowHealthContributor.remove(flowId);
	}

	@Override
	public FlowHealthIndicator getContributor(String flowId) {
		return flowHealthContributor.get(flowId);
	}

	@Override
	public Iterator<HealthContributors.Entry> iterator() {
		return flowHealthContributor.entrySet()
				.stream()
				.map((entry) -> new HealthContributors.Entry(entry.getKey(), (HealthContributor) entry.getValue()))
				.iterator();
	}

	@Override
	public Stream<HealthContributors.Entry> stream() {
		return flowHealthContributor.entrySet()
				.stream()
				.map((entry) -> new HealthContributors.Entry(entry.getKey(), (HealthContributor) entry.getValue()));
	}
}
