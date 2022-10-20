package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(MeterRegistry.class)
public class SolaceMeterConfiguration {
	@Bean
	public SolaceMessageMeterBinder solaceMessageMeterBinder() {
		return new SolaceMessageMeterBinder();
	}

	@Bean
	public SolaceMeterAccessor solaceMeterAccessor(SolaceMessageMeterBinder solaceMessageMeterBinder) {
		return new SolaceMeterAccessor(solaceMessageMeterBinder);
	}
}
