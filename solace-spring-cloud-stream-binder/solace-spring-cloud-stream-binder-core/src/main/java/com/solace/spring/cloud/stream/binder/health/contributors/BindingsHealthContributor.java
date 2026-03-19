package com.solace.spring.cloud.stream.binder.health.contributors;

import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public final class BindingsHealthContributor implements CompositeHealthContributor {
	private final Map<String, BindingHealthContributor> bindingHealthContributor = new HashMap<>();

	public void addBindingContributor(String bindingName, BindingHealthContributor bindingHealthContributor) {
		this.bindingHealthContributor.put(bindingName, bindingHealthContributor);
	}

	public void removeBindingContributor(String bindingName) {
		bindingHealthContributor.remove(bindingName);
	}

	@Override
	public BindingHealthContributor getContributor(String bindingName) {
		return bindingHealthContributor.get(bindingName);
	}

	@Override
	public Stream<HealthContributors.Entry> stream() {
		return bindingHealthContributor.entrySet()
				.stream()
				.map((entry) -> new HealthContributors.Entry(entry.getKey(), entry.getValue()));
	}
}
