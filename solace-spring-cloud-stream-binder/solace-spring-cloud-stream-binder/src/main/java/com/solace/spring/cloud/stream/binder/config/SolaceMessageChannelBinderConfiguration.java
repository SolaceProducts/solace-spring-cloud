package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceEndpointProvisioner;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.security.oauth2.client.servlet.OAuth2ClientAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Set;

import static com.solacesystems.jcsmp.XMLMessage.Outcome.ACCEPTED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.FAILED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.REJECTED;

@Configuration
@Import({SolaceHealthIndicatorsConfiguration.class, OAuth2ClientAutoConfiguration.class})
@EnableConfigurationProperties({SolaceExtendedBindingProperties.class, SolaceExtendedJavaProperties.class})
public class SolaceMessageChannelBinderConfiguration {
	private final JCSMPProperties jcsmpProperties;
	private final SolaceExtendedJavaProperties solaceExtendedJavaProperties;
	private final SolaceExtendedBindingProperties solaceExtendedBindingProperties;
	private final SolaceSessionEventHandler solaceSessionEventHandler;

	private JCSMPSession jcsmpSession;
	private Context context;

	@Nullable
	private final SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider;

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceMessageChannelBinderConfiguration.class);

	public SolaceMessageChannelBinderConfiguration(JCSMPProperties jcsmpProperties,
												   SolaceExtendedJavaProperties solaceExtendedJavaProperties,
	                                               SolaceExtendedBindingProperties solaceExtendedBindingProperties,
	                                               @Nullable SolaceSessionEventHandler eventHandler,
												   @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider) {
		this.jcsmpProperties = jcsmpProperties;
		this.solaceExtendedJavaProperties = solaceExtendedJavaProperties;
		this.solaceExtendedBindingProperties = solaceExtendedBindingProperties;
		this.solaceSessionEventHandler = eventHandler;
		this.solaceSessionOAuth2TokenProvider = solaceSessionOAuth2TokenProvider;
	}

	@PostConstruct
	private void initSession() throws JCSMPException {
		JCSMPProperties solaceJcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
		solaceJcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, new SolaceBinderClientInfoProvider());
		try {
			try {
				if (solaceSessionEventHandler != null) {
					LOGGER.debug("Registering Solace Session Event handler on session");

					SpringJCSMPFactory springJCSMPFactory =
							new SpringJCSMPFactory(solaceJcsmpProperties, solaceSessionOAuth2TokenProvider);
					context = springJCSMPFactory.createContext(new ContextProperties());
					jcsmpSession = springJCSMPFactory.createSession(context, solaceSessionEventHandler);
				} else {
					SpringJCSMPFactory springJCSMPFactory =
							new SpringJCSMPFactory(solaceJcsmpProperties, solaceSessionOAuth2TokenProvider);
					jcsmpSession = springJCSMPFactory.createSession();
				}
			} catch (JCSMPException exc) {
				if (solaceExtendedJavaProperties.isStartupFailOnConnectError()) {
					throw exc;
				}
				jcsmpSession = createJCSMPProxySession();
				return;
			}

			try {
				LOGGER.info("Connecting JCSMP session {}", jcsmpSession.getSessionName());
				jcsmpSession.connect();
				if (solaceSessionEventHandler != null) {
					// after setting the session health indicator status to UP,
					// we should not be worried about setting its status to DOWN,
					// as the call closing JCSMP session also delete the context
					// and terminates the application
					solaceSessionEventHandler.setSessionHealthUp();
				}

				if (jcsmpSession instanceof JCSMPBasicSession session &&
						!session.isRequiredSettlementCapable(Set.of(ACCEPTED, FAILED, REJECTED))) {
					LOGGER.warn("The connected Solace PubSub+ Broker is not compatible. It doesn't support message NACK capability. Consumer bindings will fail to start.");
				}
			} catch (JCSMPException e) {
				if (solaceExtendedJavaProperties.isStartupFailOnConnectError()) {
					throw e;
				}
			}
		} catch (Exception e) {
			if (context != null) {
				context.destroy();
			}
			throw e;
		}
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder(
			SolaceEndpointProvisioner solaceEndpointProvisioner,
			@Nullable ProducerMessageHandlerCustomizer<JCSMPOutboundMessageHandler> producerCustomizer,
			@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor,
			@Nullable SolaceMeterAccessor solaceMeterAccessor) {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, context, solaceEndpointProvisioner);
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		binder.setProducerMessageHandlerCustomizer(producerCustomizer);
		binder.setSolaceMeterAccessor(solaceMeterAccessor);
		binder.setSolaceBinderHealthAccessor(solaceBinderHealthAccessor);
		return binder;
	}

	@Bean
	SolaceEndpointProvisioner provisioningProvider() {
		return new SolaceEndpointProvisioner(jcsmpSession);
	}

	// Fallback logic for JCSMP session creation in case the initial creation during @PostConstruct fails (e.g., due to
	// the hostname not being resolvable at startup time). In that case, a dynamic proxy is created that will
	// retry initialization of the actual session when any method is called on the proxy, and transparently forward any
	// method calls to the actual session once it is available.
	// As soon as a real JCSMPSession is available, it will be able to handle all further retry logic by itself.

	private JCSMPSession proxySession;

	private JCSMPSession createJCSMPProxySession() {
		if (proxySession == null) {
			proxySession = (JCSMPSession) Proxy.newProxyInstance(this.getClass().getClassLoader(),
					new Class[] {JCSMPSession.class},
					new InvocationHandler() {

						@Override
						public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
							if (jcsmpSession == proxySession) {
								synchronized (this) {
									if (jcsmpSession == proxySession) {
										initSession();
									}
									if (jcsmpSession == proxySession) {
										throw new IllegalStateException("No JCSMP session available");
									}
								}
							}

							return method.invoke(jcsmpSession, arguments);
						}

					});
		}

		return proxySession;
	}

}
