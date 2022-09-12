package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(MeterRegistry.class)
public class SolaceMeterConfiguration {
	@Bean
	public MeterBinder solaceMessageMeterBinder() {
		return new SolaceMessageMeterBinder();
	}
}
