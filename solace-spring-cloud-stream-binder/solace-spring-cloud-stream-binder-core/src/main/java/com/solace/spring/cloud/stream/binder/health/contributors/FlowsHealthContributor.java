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

	public void addFlowContributor(String bindingName, FlowHealthIndicator flowHealthIndicator) {
		flowHealthContributor.put(bindingName, flowHealthIndicator);
	}

	public void removeFlowContributor(String bindingName) {
		flowHealthContributor.remove(bindingName);
	}

	@Override
	public HealthContributor getContributor(String name) {
		return flowHealthContributor.get(name);
	}

	@Override
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		return flowHealthContributor.entrySet()
				.stream()
				.map((entry) -> NamedContributor.of(entry.getKey(), (HealthContributor) entry.getValue()))
				.iterator();
	}
}
