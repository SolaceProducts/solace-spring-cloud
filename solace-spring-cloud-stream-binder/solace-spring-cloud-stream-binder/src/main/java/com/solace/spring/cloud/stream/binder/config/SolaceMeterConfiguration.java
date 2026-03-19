package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(MeterRegistry.class)
public final class SolaceMeterConfiguration {
	@Bean
	SolaceMessageMeterBinder solaceMessageMeterBinder() {
		return new SolaceMessageMeterBinder();
	}

	@Bean
	SolaceMeterAccessor solaceMeterAccessor(SolaceMessageMeterBinder solaceMessageMeterBinder) {
		return new SolaceMeterAccessor(solaceMessageMeterBinder);
	}
}
