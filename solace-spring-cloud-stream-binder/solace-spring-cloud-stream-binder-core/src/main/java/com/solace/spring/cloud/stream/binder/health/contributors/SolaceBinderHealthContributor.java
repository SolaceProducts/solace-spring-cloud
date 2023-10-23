package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		List<NamedContributor<HealthContributor>> contributors = new ArrayList<>();
		contributors.add(NamedContributor.of(CONNECTION, sessionHealthIndicator));
		contributors.add(NamedContributor.of(BINDINGS, bindingsHealthContributor));
		return contributors.iterator();
	}
}
