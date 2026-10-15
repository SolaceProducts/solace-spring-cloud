package com.solace.spring.cloud.stream.binder.util;

import com.solace.messaging.trace.propagation.MessageTracingSupport;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageReaderProperties;
import com.solace.spring.cloud.stream.binder.properties.SmfMessageWriterProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.XMLMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class XMLMessageMapperTraceContextTest {
	private static final String TRACEPARENT_1 = "00-0102030405060708090a0b0c0d0e0f10-1112131415161718-01";
	private static final String TRACESTATE_1 = "vendor1=abc";
	private static final String BAGGAGE_1 = "userId=1234";
	private static final String TRACEPARENT_2 = "00-aabbccddeeff00112233445566778899-2122232425262728-00";
	private static final String TRACESTATE_2 = "vendor2=xyz";
	private static final String BAGGAGE_2 = "userId=5678";

	private final XMLMessageMapper xmlMessageMapper = new XMLMessageMapper();

	@Mock
	private AcknowledgmentCallback acknowledgmentCallback;

	@Test
	void testTraceContextPropagationDisabledByDefault() {
		assertThat(new SolaceConsumerProperties().isTraceContextPropagation()).isFalse();
		assertThat(new SolaceProducerProperties().isTraceContextPropagation()).isFalse();
	}

	@Test
	void testTraceContextPropagationBindsFromProperty() {
		Binder binder = new Binder(new MapConfigurationPropertySource(Map.of(
				"consumer.trace-context-propagation", "true",
				"producer.trace-context-propagation", "true")));

		assertThat(binder.bind("consumer", SolaceConsumerProperties.class).get().isTraceContextPropagation()).isTrue();
		assertThat(binder.bind("producer", SolaceProducerProperties.class).get().isTraceContextPropagation()).isTrue();
	}

	@Test
	void testTraceContextRoundTripsWhenEnabled() {
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(tracedMessage(), writer(true));

		assertThat(xmlMessage.getProperties().keySet()).doesNotContain("traceparent", "tracestate", "baggage");
		MessageTracingSupport tracing = (MessageTracingSupport) xmlMessage;
		assertThat(tracing.creationContext().getTraceState()).isEqualTo(TRACESTATE_1);
		assertThat(tracing.creationContext().isSampled()).isTrue();
		assertThat(tracing.baggage().getBaggage()).isEqualTo(BAGGAGE_1);

		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(true));
		assertThat(roundTripped.getHeaders())
				.containsEntry("traceparent", TRACEPARENT_1)
				.containsEntry("tracestate", TRACESTATE_1)
				.containsEntry("baggage", BAGGAGE_1);
	}

	@Test
	void testNoTraceContextProducesNoTraceHeaders() {
		Message<?> springMessage = MessageBuilder.withPayload("no context".getBytes()).build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writer(true));
		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(true));

		assertThat(roundTripped.getHeaders()).doesNotContainKeys("traceparent", "tracestate", "baggage");
	}

	@Test
	@SuppressWarnings("unchecked")
	void testBatchEachElementCarriesItsOwnTraceContext() {
		Message<List<Object>> batchedMessage = MessageBuilder
				.withPayload(List.<Object>of("first".getBytes(), "second".getBytes()))
				.setHeader(BinderHeaders.BATCH_HEADERS, List.of(
						Map.<String, Object>of("traceparent", TRACEPARENT_1, "tracestate", TRACESTATE_1, "baggage", BAGGAGE_1),
						Map.<String, Object>of("traceparent", TRACEPARENT_2, "tracestate", TRACESTATE_2, "baggage", BAGGAGE_2)))
				.build();

		List<XMLMessage> xmlMessages = xmlMessageMapper.mapBatchedToSmf(batchedMessage, writer(true));
		Message<List<?>> roundTripped = xmlMessageMapper.mapBatchedToSpring(xmlMessages, acknowledgmentCallback, reader(true));
		List<Map<String, Object>> batchHeaders = roundTripped.getHeaders().get(BinderHeaders.BATCH_HEADERS, List.class);

		assertThat(batchHeaders).hasSize(2);
		assertThat(batchHeaders.get(0)).containsEntry("traceparent", TRACEPARENT_1)
				.containsEntry("tracestate", TRACESTATE_1)
				.containsEntry("baggage", BAGGAGE_1);
		assertThat(batchHeaders.get(1)).containsEntry("traceparent", TRACEPARENT_2)
				.containsEntry("tracestate", TRACESTATE_2)
				.containsEntry("baggage", BAGGAGE_2);
	}

	@Test
	void testProducerDisabledKeepsTraceHeadersAsUserProperties() throws SDTException {
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(tracedMessage(), writer(false));

		assertThat(xmlMessage.getProperties().getString("traceparent")).isEqualTo(TRACEPARENT_1);
		assertThat(xmlMessage.getProperties().getString("tracestate")).isEqualTo(TRACESTATE_1);
		assertThat(xmlMessage.getProperties().getString("baggage")).isEqualTo(BAGGAGE_1);
		assertThat(((MessageTracingSupport) xmlMessage).creationContext()).isNull();

		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(false));
		assertThat(roundTripped.getHeaders()).containsEntry("traceparent", TRACEPARENT_1);
	}

	@Test
	void testConsumerDisabledDoesNotExposeNativeTraceContext() {
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(tracedMessage(), writer(true));

		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(false));

		assertThat(roundTripped.getHeaders()).doesNotContainKeys("traceparent", "tracestate", "baggage");
	}

	@Test
	void testNativeTraceContextOverridesUserPropertyWhenEnabled() throws SDTException {
		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(tracedMessage(), writer(true));
		xmlMessage.getProperties().putString("traceparent", TRACEPARENT_2);

		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(true));

		assertThat(roundTripped.getHeaders()).containsEntry("traceparent", TRACEPARENT_1);
	}

	@Test
	void testBaggageWithoutTraceContextIsNotMapped() {
		Message<?> springMessage = MessageBuilder.withPayload("baggage only".getBytes())
				.setHeader("baggage", BAGGAGE_1)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writer(true));
		assertThat(((MessageTracingSupport) xmlMessage).baggage().getBaggage()).isNullOrEmpty();

		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback, reader(true));
		assertThat(roundTripped.getHeaders()).doesNotContainKeys("traceparent", "tracestate", "baggage");
	}

	@Test
	void testInvalidTraceparentIsIgnored() {
		Message<?> springMessage = MessageBuilder.withPayload("bad".getBytes())
				.setHeader("traceparent", "00-not-a-valid-traceparent")
				.setHeader("tracestate", TRACESTATE_1)
				.setHeader("baggage", BAGGAGE_1)
				.build();

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(springMessage, writer(true));

		assertThat(((MessageTracingSupport) xmlMessage).creationContext()).isNull();
		assertThat(((MessageTracingSupport) xmlMessage).baggage().getBaggage()).isNullOrEmpty();
		assertThat(xmlMessage.getProperties().keySet()).doesNotContain("traceparent", "tracestate");
	}

	@Test
	void testExcludedTraceHeadersAreNotMapped() {
		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setTraceContextPropagation(true);
		producerProperties.setHeaderExclusions(List.of("baggage"));
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setTraceContextPropagation(true);
		consumerProperties.setHeaderExclusions(List.of("tracestate"));

		XMLMessage xmlMessage = xmlMessageMapper.mapToSmf(tracedMessage(), new SmfMessageWriterProperties(producerProperties));
		Message<?> roundTripped = xmlMessageMapper.mapToSpring(xmlMessage, acknowledgmentCallback,
				new SmfMessageReaderProperties(consumerProperties));

		assertThat(roundTripped.getHeaders()).containsEntry("traceparent", TRACEPARENT_1)
				.doesNotContainKeys("tracestate", "baggage");
	}

	private static Message<?> tracedMessage() {
		return MessageBuilder.withPayload("hello".getBytes())
				.setHeader("traceparent", TRACEPARENT_1)
				.setHeader("tracestate", TRACESTATE_1)
				.setHeader("baggage", BAGGAGE_1)
				.build();
	}

	private static SmfMessageWriterProperties writer(boolean traceContextPropagation) {
		SolaceProducerProperties properties = new SolaceProducerProperties();
		properties.setTraceContextPropagation(traceContextPropagation);
		return new SmfMessageWriterProperties(properties);
	}

	private static SmfMessageReaderProperties reader(boolean traceContextPropagation) {
		SolaceConsumerProperties properties = new SolaceConsumerProperties();
		properties.setTraceContextPropagation(traceContextPropagation);
		return new SmfMessageReaderProperties(properties);
	}
}
