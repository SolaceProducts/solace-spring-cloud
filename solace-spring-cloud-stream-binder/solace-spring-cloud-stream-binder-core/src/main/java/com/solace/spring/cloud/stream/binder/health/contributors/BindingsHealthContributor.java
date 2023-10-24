package com.solace.spring.cloud.stream.binder.health.contributors;

import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BindingsHealthContributor implements CompositeHealthContributor {
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
	public Iterator<NamedContributor<HealthContributor>> iterator() {
		return bindingHealthContributor.entrySet()
				.stream()
				.map((entry) -> NamedContributor.of(entry.getKey(), (HealthContributor) entry.getValue()))
				.iterator();
	}
}
