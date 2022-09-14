package com.solace.spring.cloud.stream.binder.test.spring.configuration;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.logging.LoggingRegistryConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.List;

@TestConfiguration
public class TestMeterRegistryConfiguration {
	@Bean
	@Primary
	public CompositeMeterRegistry compositeMeterRegistry(List<MeterRegistry> meterRegistries) {
		CompositeMeterRegistry compositeMeterRegistry = new CompositeMeterRegistry();
		meterRegistries.forEach(compositeMeterRegistry::add);
		return compositeMeterRegistry;
	}

	@Bean
	public SimpleMeterRegistry simpleMeterRegistry() {
		return new SimpleMeterRegistry();
	}

	@Bean
	public LoggingMeterRegistry loggingMeterRegistry() {
		return new LoggingMeterRegistry(new LoggingRegistryConfig() {
			@Override
			public String get(String key) {
				if (key.equals(prefix() + ".step")) {
					return String.valueOf(Duration.ofMillis(500));
				} else {
					return null;
				}
			}
		}, Clock.SYSTEM);
	}
}
