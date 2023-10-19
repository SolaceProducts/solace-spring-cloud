package com.solace.spring.cloud.stream.binder.health.contributors;

import com.solace.spring.cloud.stream.binder.properties.SolaceFlowHealthProperties;
import lombok.Getter;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class BindingsHealthContributor implements CompositeHealthContributor {
	private final Map<String, BindingHealthContributor> bindingHealthContributor = new HashMap<>();

	@Getter
	private final SolaceFlowHealthProperties solaceFlowHealthProperties;

	public BindingsHealthContributor(SolaceFlowHealthProperties solaceFlowHealthProperties) {
		this.solaceFlowHealthProperties = solaceFlowHealthProperties;
	}

	public void addBindingContributor(String bindingName, BindingHealthContributor bindingHealthContributor) {
		this.bindingHealthContributor.put(bindingName, bindingHealthContributor);
	}

	public void removeBindingContributor(String bindingName) {
		bindingHealthContributor.remove(bindingName);
	}

	@Override
	public HealthContributor getContributor(String bindingName) {
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
