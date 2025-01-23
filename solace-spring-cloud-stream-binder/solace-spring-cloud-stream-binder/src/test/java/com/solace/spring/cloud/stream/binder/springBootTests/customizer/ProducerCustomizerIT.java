package com.solace.spring.cloud.stream.binder.springBootTests.customizer;

import com.solace.spring.cloud.stream.binder.outbound.JCSMPOutboundMessageHandler;
import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.stream.config.ProducerMessageHandlerCustomizer;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(classes = ProducerCustomizerIT.CustomProducerMessageHandlerCustomizationConfiguration.class)
@DirtiesContext //Ensures all listeners are stopped
public class ProducerCustomizerIT {
	private static final PubSubPlusContainer container = new PubSubPlusContainer();
	private static final JCSMPSession jcsmpSession;
	private static final String EXCLUDED_HEADER = "excluded-header";
	private static final String DESTINATION_NAME = "ProducerCustomizerIT/queue/0";

	static {
		container.start();
		JCSMPProperties props = new JCSMPProperties(container.getHost(),
				container.getMappedPort(PubSubPlusContainer.Port.SMF.getInternalPort()),
				container.getAdminUsername(), container.getAdminPassword(), null);
		try {
			jcsmpSession = JCSMPFactory.onlyInstance().createSession(props);
			jcsmpSession.connect();
		} catch (Exception e) {
			throw new RuntimeException("Failed setup", e);
		}
	}

	@AfterAll
	public static void close() {
		if (jcsmpSession != null) {
			jcsmpSession.closeSession();
		}
	}

	@DynamicPropertySource
	static void registerProperties(DynamicPropertyRegistry registry) {
		registry.add("spring.cloud.function.definition", () -> "process");
		registry.add("spring.cloud.stream.output-bindings", () -> "output-0");
		registry.add("spring.cloud.stream.bindings.output-0.destination", () -> DESTINATION_NAME);
		registry.add("spring.cloud.stream.bindings.output-0.binder", () -> "solace");
		registry.add("spring.cloud.stream.solace.bindings.output-0.producer.destination-type", () -> "queue");
		registry.add("spring.cloud.stream.solace.bindings.output-0.producer.header-exclusions", () -> EXCLUDED_HEADER);
		registry.add("solace.java.host", () -> container.getOrigin(PubSubPlusContainer.Port.SMF));
	}

	@Test
	void testProducerMessageHandlerCustomizer(@Autowired StreamBridge streamBridge) throws Exception {
		String keptHeader = "kept-header";

		Message<String> message = MessageBuilder.withPayload("test")
				.setHeader(EXCLUDED_HEADER, "abc")
				.setHeader(keptHeader, 123)
				.setHeader(CustomProducerMessageHandlerCustomizationConfiguration.PROGRAMMATICALLY_EXCLUDED_HEADER,
						"hooplah")
				.build();

		assertThat(streamBridge.send("output-0", message))
				.as("Failed to send message to output binding 'output-0'")
				.isTrue();

		FlowReceiver flow = null;
		CompletableFuture<BytesXMLMessage> smfMessageFuture = new CompletableFuture<>();
		try {
			flow = jcsmpSession.createFlow(new XMLMessageListener() {
				@Override
				public void onReceive(BytesXMLMessage bytesXMLMessage) {
					smfMessageFuture.complete(bytesXMLMessage);
				}

				@Override
				public void onException(JCSMPException e) {
					smfMessageFuture.completeExceptionally(e);
				}
			}, new ConsumerFlowProperties()
					.setEndpoint(JCSMPFactory.onlyInstance().createQueue(DESTINATION_NAME))
					.setStartState(true));

			assertThat(smfMessageFuture)
					.succeedsWithin(5, TimeUnit.MINUTES)
					.extracting(BytesXMLMessage::getProperties)
					.satisfies(
							p -> assertThat(p.containsKey(EXCLUDED_HEADER))
									.as("%s header should have been excluded", EXCLUDED_HEADER)
									.isFalse(),
							p -> assertThat(p.containsKey(
									CustomProducerMessageHandlerCustomizationConfiguration.PROGRAMMATICALLY_EXCLUDED_HEADER))
									.as("%s header should have been excluded",
											CustomProducerMessageHandlerCustomizationConfiguration.PROGRAMMATICALLY_EXCLUDED_HEADER)
									.isFalse(),
							p -> assertThat(p.get(keptHeader)).isEqualTo(123));
		} finally {
			if (flow != null) {
				flow.close();
			}
		}
	}

	@TestConfiguration
	static class CustomProducerMessageHandlerCustomizationConfiguration {
		private static final String PROGRAMMATICALLY_EXCLUDED_HEADER = "programmatically-excluded-header";

		@Bean
		public ProducerMessageHandlerCustomizer<JCSMPOutboundMessageHandler> customProducerMessageHandlerCustomizer() {
			return (handler, destinationName) ->
					handler.getSmfMessageWriterProperties().getHeaderExclusions().add(PROGRAMMATICALLY_EXCLUDED_HEADER);
		}
	}
}
