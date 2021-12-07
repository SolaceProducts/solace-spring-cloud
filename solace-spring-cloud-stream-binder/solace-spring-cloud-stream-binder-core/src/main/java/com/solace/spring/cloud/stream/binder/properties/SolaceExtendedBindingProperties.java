package com.solace.spring.cloud.stream.binder.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.binder.AbstractExtendedBindingProperties;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

import java.util.Map;

@ConfigurationProperties("spring.cloud.stream.solace")
public class SolaceExtendedBindingProperties
		extends AbstractExtendedBindingProperties<SolaceConsumerProperties,SolaceProducerProperties,SolaceBindingProperties> {

	private static final String DEFAULTS_PREFIX = "spring.cloud.stream.solace.default";

	@Override
	public String getDefaultsPrefix() {
		return DEFAULTS_PREFIX;
	}

	@Override
	public Map<String, SolaceBindingProperties> getBindings() {
		return super.doGetBindings();
	}

	@Override
	public Class<? extends BinderSpecificPropertiesProvider> getExtendedPropertiesEntryClass() {
		return SolaceBindingProperties.class;
	}
}
