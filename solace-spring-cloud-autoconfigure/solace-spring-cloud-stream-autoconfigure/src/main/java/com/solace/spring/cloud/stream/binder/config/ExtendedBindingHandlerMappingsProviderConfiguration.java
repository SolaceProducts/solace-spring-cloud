package com.solace.spring.cloud.stream.binder.config;

import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.cloud.stream.config.BindingHandlerAdvise.MappingsProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ExtendedBindingHandlerMappingsProviderConfiguration {

	@Bean
	public MappingsProvider solaceExtendedPropertiesDefaultMappingsProvider() {
		return () -> {
			Map<ConfigurationPropertyName, ConfigurationPropertyName> mappings = new HashMap<>();
			mappings.put(ConfigurationPropertyName.of("spring.cloud.stream.solace.bindings"),
					ConfigurationPropertyName.of("spring.cloud.stream.solace.default"));
			return mappings;
		};
	}
}
