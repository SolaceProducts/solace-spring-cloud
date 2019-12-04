package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoCloudConfiguration;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.boot.autoconfigure.SolaceJavaProperties;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.Cloud;
import org.springframework.cloud.CloudFactory;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({ SolaceMessageChannelBinderConfiguration.class })
public class SolaceServiceAutoConfiguration {

	/**
	 * Configuration used when:
	 * <ul>
	 *     <li>Not using cloud profile</li>
	 * </ul>
	 * Defers to Spring Boot auto-configuration.
	 */
	@Configuration
	@Profile("!cloud")
	@Import({SolaceJavaAutoCloudConfiguration.class, SolaceJavaAutoConfiguration.class})
	protected static class NoCloudProfile{}

	@Configuration
	@Profile("cloud")
	protected static class CloudProfile {

		/**
		 * Configuration used when:
		 * <ul>
		 *     <li>Using cloud profile</li>
		 *     <li>No cloud connectors are found on the classpath</li>
		 * </ul>
		 * Defers to Spring Boot auto-configuration.
		 */
		@Configuration
		@ConditionalOnMissingClass("org.springframework.cloud.Cloud")
		@Import({SolaceJavaAutoCloudConfiguration.class, SolaceJavaAutoConfiguration.class})
		protected static class NoCloudConnectors {}

		@Configuration
		@ConditionalOnClass(Cloud.class)
		protected static class CloudConnectors {

			/**
			 * Configuration used when:
			 * <ul>
			 *     <li>Using cloud profile</li>
			 *     <li>Cloud connectors are found on the classpath</li>
			 *     <li>spring.cloud.stream.overrideCloudConnectors property is false or missing</li>
			 * </ul>
			 * Defers to Spring Boot auto-configuration.
			 */
			@Configuration
			@ConditionalOnProperty(value = "spring.cloud.stream.overrideCloudConnectors", havingValue = "false", matchIfMissing = true)
			@Import({SolaceJavaAutoCloudConfiguration.class, SolaceJavaAutoConfiguration.class})
			protected static class UseCloudConnectors {}

			/**
			 * Configuration used when:
			 * <ul>
			 *     <li>Using cloud profile</li>
			 *     <li>Cloud connectors are found on the classpath</li>
			 *     <li>spring.cloud.stream.overrideCloudConnectors property is true</li>
			 * </ul>
			 * Defers to Spring Boot auto-configuration.
			 */
			@Configuration
			@ConditionalOnProperty(value = "spring.cloud.stream.overrideCloudConnectors")
			@EnableConfigurationProperties(SolaceJavaProperties.class)
			protected static class OverrideCloudConnectors {
				/* TODO Make this class a dummy implementation of SolaceJavaAutoConfigurationBase
				 * The result would be the same, but without the need to maintain the same code in two places...
				 * Task will only be achievable once SolaceJavaAutoConfigurationBase becomes public.
				 */
				@Bean
				@Primary
				SpringJCSMPFactory getSpringJCSMPFactory(Cloud cloud, SolaceJavaProperties properties) {
					// This is a straight copy-paste from SolaceJavaAutoConfigurationBase.getSpringJCSMPFactory.
					Properties apiProps = new Properties();
					Set<Map.Entry<String,String>> set = properties.getApiProperties().entrySet();
					for (Map.Entry<String,String> entry : set) {
						apiProps.put("jcsmp." + entry.getKey(), entry.getValue());
					}

					JCSMPProperties jcsmpProps = apiProps != null ? JCSMPProperties.fromProperties(apiProps) :
							new JCSMPProperties();
					jcsmpProps.setProperty(JCSMPProperties.HOST, properties.getHost());
					jcsmpProps.setProperty(JCSMPProperties.VPN_NAME, properties.getMsgVpn());
					jcsmpProps.setProperty(JCSMPProperties.USERNAME, properties.getClientUsername());
					jcsmpProps.setProperty(JCSMPProperties.PASSWORD, properties.getClientPassword());

					if ((properties.getClientName() != null) && (!properties.getClientName().isEmpty())) {
						jcsmpProps.setProperty(JCSMPProperties.CLIENT_NAME, properties.getClientName());
					}

					// Channel Properties
					JCSMPChannelProperties cp = (JCSMPChannelProperties) jcsmpProps
							.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
					cp.setConnectRetries(properties.getConnectRetries());
					cp.setReconnectRetries(properties.getReconnectRetries());
					cp.setConnectRetriesPerHost(properties.getConnectRetriesPerHost());
					cp.setReconnectRetryWaitInMillis(properties.getReconnectRetryWaitInMillis());

					return new SpringJCSMPFactory(jcsmpProps);
				}
			}

			@Bean
			@ConditionalOnMissingBean
			public Cloud cloud() {
				return new CloudFactory().getCloud();
			}
		}
	}
}
