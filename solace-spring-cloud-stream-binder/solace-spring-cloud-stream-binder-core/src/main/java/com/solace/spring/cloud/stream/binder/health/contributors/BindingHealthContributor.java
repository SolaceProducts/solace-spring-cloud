package com.solace.spring.cloud.stream.binder.health.contributors;

import java.util.stream.Stream;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

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
  public Stream<HealthContributors.Entry> stream() {
    return Stream.of(new HealthContributors.Entry(FLOWS, flowsHealthContributor));
  }

	public FlowsHealthContributor getFlowsHealthContributor() {
		return flowsHealthContributor;
	}
}
