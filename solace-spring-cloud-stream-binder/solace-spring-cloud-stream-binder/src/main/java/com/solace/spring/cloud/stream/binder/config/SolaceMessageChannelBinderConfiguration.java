package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceEndpointProvisioner;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import jakarta.annotation.PostConstruct;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;

import java.util.Set;

import static com.solacesystems.jcsmp.XMLMessage.Outcome.ACCEPTED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.FAILED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.REJECTED;

@Configuration
@Import(SolaceHealthIndicatorsConfiguration.class)
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
	                                               @Nullable SolaceSessionEventHandler eventHandler) {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceSessionEventHandler = eventHandler;
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

			if (jcsmpSession instanceof JCSMPBasicSession session &&
					!session.isRequiredSettlementCapable(Set.of(ACCEPTED,FAILED,REJECTED))) {
				logger.warn("The connected Solace PubSub+ Broker is not compatible. It doesn't support message NACK capability. Consumer bindings will fail to start.");
			}
		} catch (Exception e) {
			if (context != null) {
				context.destroy();
			}
			throw e;
		}
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder(SolaceEndpointProvisioner solaceEndpointProvisioner,
														  @Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor,
														  @Nullable SolaceMeterAccessor solaceMeterAccessor) {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, context, solaceEndpointProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		if (solaceBinderHealthAccessor != null) {
			binder.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
		}
		return binder;
	}

	@Bean
	SolaceEndpointProvisioner provisioningProvider() {
		return new SolaceEndpointProvisioner(jcsmpSession);
	}

}
