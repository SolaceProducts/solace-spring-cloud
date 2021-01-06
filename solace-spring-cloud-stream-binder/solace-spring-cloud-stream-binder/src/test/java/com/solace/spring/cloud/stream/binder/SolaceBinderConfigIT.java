package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.config.SolaceServiceAutoConfiguration;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnClient;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Binder {@link Configuration @Configuration} testing.
 *
 * These are <b>NOT</b> tests regarding {@link SolaceMessageChannelBinder}.
 */
@RunWith(InheritedTestsFilteredRunner.class)
@ContextConfiguration(classes = {SolaceServiceAutoConfiguration.class},
		initializers = {ConfigFileApplicationContextInitializer.class, SolaceBinderConfigIT.Initializer.class})
@IgnoreInheritedTests
public class SolaceBinderConfigIT extends SolaceBinderITBase {
	@Autowired
	private JCSMPProperties jcsmpProperties;

	@Test
	public void testClientInfoProvider() throws Exception {
		String vpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		String clientName = jcsmpProperties.getStringProperty(JCSMPProperties.CLIENT_NAME);
		MonitorMsgVpnClient client = sempV2Api.monitor()
				.getMsgVpnClient(vpnName, clientName, null)
				.getData();

		Pattern versionPattern = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+");
		Pattern datePattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(client.getSoftwareVersion())
				.matches(String.format("%s(?:-SNAPSHOT)? \\(%s\\)", versionPattern, versionPattern));
		softly.assertThat(client.getSoftwareDate()).matches(String.format("%s \\(%s\\)", datePattern, datePattern));
		softly.assertThat(client.getPlatform()).endsWith("Solace Spring Cloud Stream Binder (JCSMP SDK)");
		softly.assertAll();
	}

	static class Initializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
		@Override
		public void initialize(ConfigurableApplicationContext configurableApplicationContext) {
			String clientName = SolaceBinderConfigIT.class.getName() + UUID.randomUUID().toString();
			TestPropertyValues.of(
						String.format("solace.java.apiProperties.%s=%s", JCSMPProperties.CLIENT_NAME, clientName))
					.applyTo(configurableApplicationContext.getEnvironment());
		}
	}
}
