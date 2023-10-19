package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.health.contributors.ConnectionHealthContributor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import jakarta.annotation.PostConstruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;

@Configuration
@Import(HealthIndicatorsConfiguration.class)
@EnableConfigurationProperties({SolaceExtendedBindingProperties.class})
public class SolaceMessageChannelBinderConfiguration {
	private final JCSMPProperties jcsmpProperties;
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;
	private final SolaceSessionEventHandler solaceSessionEventHandler;

	private JCSMPSession jcsmpSession;
	private Context context;

	private static final Log logger = LogFactory.getLog(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(JCSMPProperties jcsmpProperties,
	                                               SolaceExtendedBindingProperties solaceExtendedBindingProperties,
	                                               @Nullable SolaceSessionEventHandler eventHandler) throws JCSMPException {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceSessionEventHandler = eventHandler;
		this.initSession();
	}

	@PostConstruct
	private void initSession() throws JCSMPException {
		JCSMPProperties jcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
		jcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, new SolaceBinderClientInfoProvider());
		try {
			if (solaceSessionEventHandler != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("Registering Solace Session Event handler on session");
				}
				context = JCSMPFactory.onlyInstance().createContext(new ContextProperties());
				jcsmpSession = JCSMPFactory.onlyInstance().createSession(jcsmpProperties, context, solaceSessionEventHandler);
			} else {
				jcsmpSession = JCSMPFactory.onlyInstance().createSession(jcsmpProperties);
			}
			logger.info(String.format("Connecting JCSMP session %s", jcsmpSession.getSessionName()));
			jcsmpSession.connect();
			if (solaceSessionEventHandler != null) {
				// after setting the session health indicator status to UP,
				// we should not be worried about setting its status to DOWN,
				// as the call closing JCSMP session also delete the context
				// and terminates the application
				solaceSessionEventHandler.setSessionHealthUp();
			}
		} catch (Exception e) {
			if (context != null) {
				context.destroy();
			}
			throw e;
		}
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder(SolaceQueueProvisioner solaceQueueProvisioner,
	                                                      @Nullable ConnectionHealthContributor connectionHealthContributor,
	                                                      @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, context, solaceQueueProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		if (connectionHealthContributor != null) {
			binder.setBindingsHealthContributor(connectionHealthContributor.getSolaceBindingsHealthContributor());
		}
		return binder;
	}

	@Bean
	SolaceQueueProvisioner provisioningProvider() {
		return new SolaceQueueProvisioner(jcsmpSession);
	}

}
