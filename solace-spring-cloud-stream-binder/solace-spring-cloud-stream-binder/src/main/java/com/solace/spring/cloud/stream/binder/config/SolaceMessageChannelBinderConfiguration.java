package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.util.SolaceHealthIndicator;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@EnableConfigurationProperties({ SolaceExtendedBindingProperties.class })
public class SolaceMessageChannelBinderConfiguration {
	private final JCSMPProperties jcsmpProperties;
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;

	private JCSMPSession jcsmpSession;

	private SolaceHealthIndicator solaceHealthIndicator = new SolaceHealthIndicator();

	private static final Log logger = LogFactory.getLog(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(JCSMPProperties jcsmpProperties,
												   SolaceExtendedBindingProperties solaceExtendedBindingProperties) {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
	}

	@PostConstruct
	private void initSession() throws JCSMPException {
		JCSMPProperties jcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
		jcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, new SolaceBinderClientInfoProvider());
		jcsmpSession = JCSMPFactory.onlyInstance().createSession(jcsmpProperties, null, solaceHealthIndicator);
		logger.info(String.format("Connecting JCSMP session %s", jcsmpSession.getSessionName()));
		jcsmpSession.connect();
		solaceHealthIndicator.connected();
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder() throws Exception {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, provisioningProvider());
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		return binder;
	}

	@Bean
	SolaceQueueProvisioner provisioningProvider() {
		return new SolaceQueueProvisioner(jcsmpSession);
	}

	@Configuration
	@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
	public class SolaceHealthIndicatorConfiguration {
		@Bean
		public HealthIndicator binderHealthIndicator() {
			return solaceHealthIndicator;
		}
	}

}
