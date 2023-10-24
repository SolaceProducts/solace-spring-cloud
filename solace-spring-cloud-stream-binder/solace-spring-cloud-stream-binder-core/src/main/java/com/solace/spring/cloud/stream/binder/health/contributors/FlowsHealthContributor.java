package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		return flowHealthContributor.entrySet()
				.stream()
				.map((entry) -> NamedContributor.of(entry.getKey(), (HealthContributor) entry.getValue()))
				.iterator();
	}
}
