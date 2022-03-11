package com.solace.spring.cloud.stream.binder.test.junit.extension.pubsubplus.provider;

import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PubSubPlusSpringProvider implements PubSubPlusExtension.ExternalProvider {
	private static final Logger LOGGER = LoggerFactory.getLogger(PubSubPlusSpringProvider.class);

	@Override
	public boolean isValid(ExtensionContext extensionContext) {
		ApplicationContext applicationContext = SpringExtension.getApplicationContext(extensionContext);
		Environment contextEnvironment = applicationContext.getEnvironment();
		try {
			applicationContext.getBean(JCSMPProperties.class);
		} catch (NoSuchBeanDefinitionException e) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Didn't find required bean: " + JCSMPProperties.class, e);
			}
			return false;
		}

		if (!Stream.of(TestProperties.values()).map(TestProperties::getProperty)
				.allMatch(contextEnvironment::containsProperty)) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Didn't find required test properties: [{}]", Stream.of(TestProperties.values())
						.map(TestProperties::getProperty)
						.collect(Collectors.joining(", ")));
			}
			return false;
		}
		return true;
	}

	@Override
	public void init(ExtensionContext extensionContext) {
		// Do not cache anything in the root store.
		// There are tests which modify the config at runtime and caching will conflict with those tests.
	}

	@Override
	public JCSMPProperties createJCSMPProperties(ExtensionContext extensionContext) {
		return SpringExtension.getApplicationContext(extensionContext).getBean(JCSMPProperties.class);
	}

	@Override
	public SempV2Api createSempV2Api(ExtensionContext extensionContext) {
		Environment contextEnvironment = SpringExtension.getApplicationContext(extensionContext).getEnvironment();
		String mgmtHost = contextEnvironment.getRequiredProperty(TestProperties.MGMT_HOST.getProperty());
		String mgmtUsername = contextEnvironment.getRequiredProperty(TestProperties.MGMT_USERNAME.getProperty());
		String mgmtPassword = contextEnvironment.getRequiredProperty(TestProperties.MGMT_PASSWORD.getProperty());
		return new SempV2Api(mgmtHost, mgmtUsername, mgmtPassword);
	}

	private enum TestProperties {
		MGMT_HOST("test.solace.mgmt.host"),
		MGMT_USERNAME("test.solace.mgmt.username"),
		MGMT_PASSWORD("test.solace.mgmt.password");

		private final String property;

		TestProperties(String propertyName) {
			this.property = propertyName;
		}

		public String getProperty() {
			return property;
		}
	}
}
