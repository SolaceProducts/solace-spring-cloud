package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import org.springframework.boot.health.contributor.CompositeHealthContributor;
import org.springframework.boot.health.contributor.HealthContributor;
import org.springframework.boot.health.contributor.HealthContributors;

import java.util.stream.Stream;

public final class SolaceBinderHealthContributor implements CompositeHealthContributor {
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
	public Stream<HealthContributors.Entry> stream() {
		return Stream.of(
				new HealthContributors.Entry(CONNECTION, sessionHealthIndicator),
				new HealthContributors.Entry(BINDINGS, bindingsHealthContributor)
		);
	}
}
