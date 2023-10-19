package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnClient;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Binder {@link Configuration @Configuration} testing.
 *
 * These are <b>NOT</b> tests regarding {@link SolaceMessageChannelBinder}.
 */
@Isolated
@Execution(ExecutionMode.SAME_THREAD)
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
public class SolaceBinderConfigIT {
	private SolaceMessageChannelBinderConfiguration binderConfiguration;
	private String clientName;

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties, ApplicationContext applicationContext, TestInfo testInfo) throws JCSMPException {
		clientName = UUID.randomUUID().toString();
		jcsmpProperties.setProperty(JCSMPProperties.CLIENT_NAME, clientName);

		binderConfiguration = new SolaceMessageChannelBinderConfiguration(jcsmpProperties,
				new SolaceExtendedBindingProperties(), null);
		AutowireCapableBeanFactory beanFactory = applicationContext.getAutowireCapableBeanFactory();
		beanFactory.autowireBean(binderConfiguration);
		binderConfiguration = (SolaceMessageChannelBinderConfiguration) beanFactory.initializeBean(binderConfiguration,
				testInfo.toString());
	}

	@Test
	public void testClientInfoProvider(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api, SoftAssertions softly)
			throws Exception {
		MonitorMsgVpnClient client;
		SolaceMessageChannelBinder solaceMessageChannelBinder = binderConfiguration.solaceMessageChannelBinder(
				binderConfiguration.provisioningProvider(), null, null);
		try {
			String vpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
			client = sempV2Api.monitor()
					.getMsgVpnClient(vpnName, clientName, null)
					.getData();
		} finally {
			solaceMessageChannelBinder.destroy();
		}

		Pattern versionPattern = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+");
		Pattern datePattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");

		softly.assertThat(client.getSoftwareVersion())
				.matches(String.format("%s(?:-SNAPSHOT)? \\(%s\\)", versionPattern, versionPattern));
		softly.assertThat(client.getSoftwareDate()).matches(String.format("%s \\(%s\\)", datePattern, datePattern));
		softly.assertThat(client.getPlatform()).endsWith("Solace Spring Cloud Stream Binder (JCSMP SDK)");
	}
}
