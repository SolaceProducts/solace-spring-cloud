package com.solace.spring.cloud.stream.binder.health.contributors;

import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class BindingHealthContributor implements CompositeHealthContributor {
	private final FlowsHealthContributor flowsHealthContributor;
	private static final String FLOWS = "flows";

	public BindingHealthContributor(FlowsHealthContributor flowsHealthContributor) {
		this.flowsHealthContributor = flowsHealthContributor;
	}

	@Override
	public HealthContributor getContributor(String name) {
		return name.equals(FLOWS) ? flowsHealthContributor : null;
	}

	@Override
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		Set<NamedContributor<HealthContributor>> contributors = Collections
				.singleton(NamedContributor.of(FLOWS, flowsHealthContributor));
		return contributors.iterator();
	}

	public FlowsHealthContributor getFlowsHealthContributor() {
		return flowsHealthContributor;
	}
}
