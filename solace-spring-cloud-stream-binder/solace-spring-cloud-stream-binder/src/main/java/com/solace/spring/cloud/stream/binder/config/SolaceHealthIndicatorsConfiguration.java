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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
	private static final Log logger = LogFactory.getLog(SolaceHealthIndicatorsConfiguration.class);

	@Bean
	public SolaceBinderHealthAccessor solaceBinderHealthAccessor(
			SolaceBinderHealthContributor solaceBinderHealthContributor) {
		return new SolaceBinderHealthAccessor(solaceBinderHealthContributor);
	}

	@Bean
	public SolaceBinderHealthContributor solaceBinderHealthContributor(
			SolaceSessionHealthProperties solaceSessionHealthProperties) {
		if (logger.isDebugEnabled()) {
			logger.debug("Creating Solace Connection Health Indicators Hierarchy");
		}
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
		if (logger.isDebugEnabled()) {
			logger.debug("Creating Solace Session Event Handler for monitoring Health");
		}
		return new SolaceSessionEventHandler(jcsmpProperties, solaceSessionOAuth2TokenProvider, healthContributor.getSolaceSessionHealthIndicator());
	}

}
