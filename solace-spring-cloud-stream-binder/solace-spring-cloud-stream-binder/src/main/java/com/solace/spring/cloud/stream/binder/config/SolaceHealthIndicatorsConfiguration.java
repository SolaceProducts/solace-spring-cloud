package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.health.contributors.BindingsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.SolaceBinderHealthContributor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
@EnableConfigurationProperties({SolaceSessionHealthProperties.class})
public class SolaceHealthIndicatorsConfiguration {
	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceHealthIndicatorsConfiguration.class);

	@Bean
	public SolaceBinderHealthAccessor solaceBinderHealthAccessor(
			SolaceBinderHealthContributor solaceBinderHealthContributor) {
		return new SolaceBinderHealthAccessor(solaceBinderHealthContributor);
	}

	@Bean
	public SolaceBinderHealthContributor solaceBinderHealthContributor(
			SolaceSessionHealthProperties solaceSessionHealthProperties) {
		LOGGER.debug("Creating Solace Connection Health Indicators Hierarchy");
		return new SolaceBinderHealthContributor(
				new SessionHealthIndicator(solaceSessionHealthProperties),
				new BindingsHealthContributor()
		);
	}

	@Bean
	public SolaceSessionEventHandler solaceSessionEventHandler(
			JCSMPProperties jcsmpProperties,
			@Nullable  SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider,
			SolaceBinderHealthContributor healthContributor) {
		LOGGER.debug("Creating Solace Session Event Handler for monitoring Health");
		return new SolaceSessionEventHandler(jcsmpProperties, solaceSessionOAuth2TokenProvider, healthContributor.getSolaceSessionHealthIndicator());
	}

}
