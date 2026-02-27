package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class SolaceBinderHealthContributor implements CompositeHealthContributor {
	private final SessionHealthIndicator sessionHealthIndicator;
	private final BindingsHealthContributor bindingsHealthContributor;
	private static final String CONNECTION = "connection";
	private static final String BINDINGS = "bindings";

	public SolaceBinderHealthContributor(SessionHealthIndicator sessionHealthIndicator,
	                                     BindingsHealthContributor bindingsHealthContributor) {
		this.sessionHealthIndicator = sessionHealthIndicator;
		this.bindingsHealthContributor = bindingsHealthContributor;
	}

	@Override
	public HealthContributor getContributor(String name) {
		return switch (name) {
			case CONNECTION -> sessionHealthIndicator;
			case BINDINGS -> bindingsHealthContributor;
			default -> null;
		};
	}

	public SessionHealthIndicator getSolaceSessionHealthIndicator() {
		return sessionHealthIndicator;
	}

	public BindingsHealthContributor getSolaceBindingsHealthContributor() {
		return bindingsHealthContributor;
	}

	@Override
	public Iterator<HealthContributors.Entry> iterator() {
		List<HealthContributors.Entry> contributors = new ArrayList<>();
		contributors.add(new HealthContributors.Entry(CONNECTION, sessionHealthIndicator));
		contributors.add(new HealthContributors.Entry(BINDINGS, bindingsHealthContributor));
		return contributors.iterator();
	}

	@Override
	public Stream<HealthContributors.Entry> stream() {
		return Stream.of(
				new HealthContributors.Entry(CONNECTION, sessionHealthIndicator),
				new HealthContributors.Entry(BINDINGS, bindingsHealthContributor)
		);
	}
}
