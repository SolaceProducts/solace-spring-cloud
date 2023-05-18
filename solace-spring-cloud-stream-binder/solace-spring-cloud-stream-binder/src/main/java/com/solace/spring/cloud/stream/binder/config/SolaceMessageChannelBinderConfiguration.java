package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionEventHandler;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;

import jakarta.annotation.PostConstruct;

@Configuration
@Import(SolaceBinderHealthIndicatorConfiguration.class)
@EnableConfigurationProperties({ SolaceExtendedBindingProperties.class })
public class SolaceMessageChannelBinderConfiguration {
	private final JCSMPProperties jcsmpProperties;
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;
	private final SolaceSessionEventHandler solaceSessionEventHandler;

	private JCSMPSession jcsmpSession;
	private Context context;

	private static final Log logger = LogFactory.getLog(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(JCSMPProperties jcsmpProperties,
												   SolaceExtendedBindingProperties solaceExtendedBindingProperties,
												   @Nullable SolaceSessionEventHandler solaceSessionEventHandler) {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceSessionEventHandler = solaceSessionEventHandler;
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
				solaceSessionEventHandler.connected();
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
														  @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, context, solaceQueueProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		return binder;
	}

	@Bean
	SolaceQueueProvisioner provisioningProvider() {
		return new SolaceQueueProvisioner(jcsmpSession);
	}

}
