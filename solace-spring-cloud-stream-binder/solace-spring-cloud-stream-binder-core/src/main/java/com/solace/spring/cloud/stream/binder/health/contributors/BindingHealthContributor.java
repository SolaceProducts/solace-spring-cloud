package com.solace.spring.cloud.stream.binder.health.contributors;

import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

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
	public Iterator<HealthContributors.Entry> iterator() {
		Set<HealthContributors.Entry> contributors = Collections
				.singleton(new HealthContributors.Entry(FLOWS, flowsHealthContributor));
		return contributors.iterator();
	}

	@Override
	public Stream<HealthContributors.Entry> stream() {
		return Stream.of(new HealthContributors.Entry(FLOWS, flowsHealthContributor));
	}

	public FlowsHealthContributor getFlowsHealthContributor() {
		return flowsHealthContributor;
	}
}
