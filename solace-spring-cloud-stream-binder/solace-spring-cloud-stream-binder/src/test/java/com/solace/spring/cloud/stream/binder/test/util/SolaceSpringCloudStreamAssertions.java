package com.solace.spring.cloud.stream.binder.test.util;

import com.solace.spring.cloud.stream.binder.health.contributors.BindingHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.BindingsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.FlowsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessage;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Statistic;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowingConsumer;
import org.springframework.boot.actuate.health.NamedContributor;
import org.springframework.boot.actuate.health.Status;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.util.MimeType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

/**
 * Assertions to validate Spring Cloud Stream Binder for Solace.
 */
public class SolaceSpringCloudStreamAssertions {
	/**
	 * <p>Returns a function to evaluate a message for a header which may be nested in a batched message.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param header header key
	 * @param type header type
	 * @param isBatched is message expected to be a batched message?
	 * @param requirements requirements which the header value must satisfy. See
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer)}.
	 * @param <T> header type
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return message header requirements evaluator
	 */
	public static <T> ThrowingConsumer<Message<?>> hasNestedHeader(String header, Class<T> type, boolean isBatched,
																   ThrowingConsumer<T> requirements) {
		return message -> {
			ThrowingConsumer<Map<String, Object>> satisfiesHeader = msgHeaders -> assertThat(msgHeaders.get(header))
					.isInstanceOf(type)
					.satisfies(headerValue -> requirements.accept(type.cast(headerValue)));

			if (isBatched) {
				assertThat(message.getHeaders())
						.extractingByKey(SolaceBinderHeaders.BATCHED_HEADERS)
						.isNotNull()
						.isInstanceOf(List.class)
						.asList()
						.isNotEmpty()
						.allSatisfy(msgHeaders -> assertThat(msgHeaders)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
								.satisfies(satisfiesHeader));
			} else {
				assertThat(message.getHeaders()).satisfies(satisfiesHeader);
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate a message for the lack of a header which may be nested in a batched message.
	 * </p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param header header key
	 * @param isBatched is message expected to be a batched message?
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return message header requirements evaluator
	 */
	public static ThrowingConsumer<Message<?>> noNestedHeader(String header, boolean isBatched) {
		return message -> {
			if (isBatched) {
				assertThat(message.getHeaders())
						.extractingByKey(SolaceBinderHeaders.BATCHED_HEADERS)
						.isNotNull()
						.isInstanceOf(List.class)
						.asList()
						.isNotEmpty()
						.allSatisfy(msgHeaders -> assertThat(msgHeaders)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
								.doesNotContainKey(header));
			} else {
				assertThat(message.getHeaders()).doesNotContainKey(header);
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate that a consumed Solace message is valid.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param consumerProperties consumer properties
	 * @param expectedMessages the messages against which this message will be evaluated against.
	 *                            Should have a size of exactly 1 if this consumer is not in batch mode.
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return message evaluator
	 */
	public static ThrowingConsumer<Message<?>> isValidMessage(
			ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
			List<Message<?>> expectedMessages) {
		return isValidMessage(consumerProperties, expectedMessages.toArray(new Message<?>[0]));
	}

	/**
	 * Same as {@link #isValidMessage(ExtendedConsumerProperties, List)}.
	 * @param consumerProperties consumer properties
	 * @param expectedMessages the messages against which this message will be evaluated against.
	 *                            Should have a size of exactly 1 if this consumer is not in batch mode.
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @see #isValidMessage(ExtendedConsumerProperties, List)
	 * @return message evaluator
	 */
	public static ThrowingConsumer<Message<?>> isValidMessage(
			ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
			Message<?>... expectedMessages) {
		// content-type header may be a String or MimeType
		Function<Object, MimeType> convertToMimeType = v -> v instanceof MimeType ? (MimeType) v :
				MimeType.valueOf(v.toString());
		MimeType expectedContentType = Optional.ofNullable(expectedMessages[0].getHeaders()
				.get(MessageHeaders.CONTENT_TYPE))
				.map(convertToMimeType)
				.orElse(null);

		return message -> {
			if (consumerProperties.isBatchMode()) {
				assertThat(message.getHeaders())
						.containsKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK)
						.containsKey(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT)
						.extractingByKey(SolaceBinderHeaders.BATCHED_HEADERS)
						.isNotNull()
						.isInstanceOf(List.class)
						.asList()
						.hasSize(expectedMessages.length)
						.allSatisfy(msgHeaders -> assertThat(msgHeaders)
								.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
								.doesNotContainKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK)
								.doesNotContainKey(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT)
								.hasEntrySatisfying(MessageHeaders.CONTENT_TYPE, contentType ->
										assertThat(convertToMimeType.apply(contentType))
												.isEqualTo(expectedContentType)));

				assertThat(message.getPayload())
						.isInstanceOf(List.class)
						.asList()
						.containsExactly(Arrays.stream(expectedMessages).map(Message::getPayload).toArray());
			} else {
				assertThat(message.getPayload()).isEqualTo(expectedMessages[0].getPayload());
				assertThat(StaticMessageHeaderAccessor.getContentType(message)).isEqualTo(expectedContentType);
				assertThat(message.getHeaders())
						.containsKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK)
						.containsKey(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT);
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate that an error message is valid.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param expectRawMessageHeader true if the error message contains the raw XMLMessage
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return message evaluator
	 */
	public static ThrowingConsumer<Message<?>> isValidProducerErrorMessage(boolean expectRawMessageHeader) {
		return errorMessage -> {
			assertThat(errorMessage.getPayload()).isNotNull();
			assertThat(errorMessage)
					.asInstanceOf(InstanceOfAssertFactories.type(ErrorMessage.class))
					.extracting(ErrorMessage::getOriginalMessage)
					.isNotNull();
			if (expectRawMessageHeader) {
				assertThat((Object) StaticMessageHeaderAccessor.getSourceData(errorMessage))
						.isInstanceOf(XMLMessage.class);
			} else {
				assertThat(errorMessage.getHeaders())
						.doesNotContainKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate that a consumed Solace message is valid.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param consumerProperties consumer properties
	 * @param pollableConsumer true if consumer is a pollable consumer
	 * @param expectRawMessageHeader true if the error message contains the raw XMLMessage
	 * @param expectedMessages the messages against which this message will be evaluated against.
	 *                            Should have a size of exactly 1 if this consumer is not in batch mode.
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return message evaluator
	 */
	public static ThrowingConsumer<Message<?>> isValidConsumerErrorMessage(
			ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
			boolean pollableConsumer,
			boolean expectRawMessageHeader,
			List<Message<?>> expectedMessages) {
		return errorMessage -> {
			assertThat(errorMessage.getPayload()).isNotNull();
			assertThat(errorMessage)
					.asInstanceOf(InstanceOfAssertFactories.type(ErrorMessage.class))
					.extracting(ErrorMessage::getOriginalMessage)
					.isNotNull()
					.satisfies(isValidMessage(consumerProperties, expectedMessages))
					.extracting(Message::getHeaders)
					.asInstanceOf(InstanceOfAssertFactories.map(String.class, Object.class))
					.hasEntrySatisfying(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT, deliveryAttempt ->
							assertThat(deliveryAttempt)
									.asInstanceOf(InstanceOfAssertFactories.ATOMIC_INTEGER)
									.hasValue(pollableConsumer ? 0 : consumerProperties.getMaxAttempts()));

			if (expectRawMessageHeader) {
				if (consumerProperties.isBatchMode()) {
					assertThat((Object) StaticMessageHeaderAccessor.getSourceData(errorMessage))
							.isNotNull()
							.asList()
							.allSatisfy(m -> assertThat(m).isInstanceOf(XMLMessage.class));
				} else {
					assertThat((Object) StaticMessageHeaderAccessor.getSourceData(errorMessage))
							.isInstanceOf(XMLMessage.class);
				}
			} else {
				assertThat(errorMessage.getHeaders())
						.doesNotContainKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
			}
		};
	}

	/**
	 * <p>Returns a function which drains and evaluates the messages for the provided error queue name.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param jcsmpSession JCSMP session
	 * @param expectedMessages expected messages in error queue
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return error queue evaluator
	 */
	@SuppressWarnings("CatchMayIgnoreException")
	public static ThrowingConsumer<String> errorQueueHasMessages(JCSMPSession jcsmpSession,
																 List<Message<?>> expectedMessages) {
		return errorQueueName -> {
			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(errorQueueName));
			errorQueueFlowProperties.setStartState(true);
			FlowReceiver flowReceiver = null;
			SoftAssertions softly = new SoftAssertions();
			try {
				flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
				for (Message<?> message : expectedMessages) {
					BytesXMLMessage errorQueueMessage = flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10));
					if (errorQueueMessage == null) {
						throw new TimeoutException(String.format(
								"Timed out while waiting for messages from error queue %s", errorQueueName));
					}
					softly.assertThat(errorQueueMessage).satisfies(msg -> {
						assertThat(msg).isInstanceOf(BytesMessage.class);
						assertThat(((BytesMessage) msg).getData()).isEqualTo(message.getPayload());
					});
				}
			} catch (Throwable e) {
				softly.fail("unexpected exception thrown: " + e.getMessage(), e);
			} finally {
				if (flowReceiver != null) {
					flowReceiver.close();
				}
				softly.assertAll();
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate that a Solace message size meter is valid.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[]) satisfies(ThrowingConsumer[])}.</p>
	 * @param nameTagValue value of the name tag
	 * @param value expected {@link Statistic#TOTAL}
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer[])
	 * @return meter evaluator
	 */
	public static ThrowingConsumer<Meter> isValidMessageSizeMeter(String nameTagValue, double value) {
		return meter -> assertThat(meter).satisfies(
				m -> assertThat(m.getId())
						.as("Checking ID for meter %s", meter)
						.satisfies(
								meterId -> assertThat(meterId.getType()).isEqualTo(Meter.Type.DISTRIBUTION_SUMMARY),
								meterId -> assertThat(meterId.getBaseUnit()).isEqualTo("bytes"),
								meterId -> assertThat(meterId.getTags())
										.hasSize(1)
										.first()
										.satisfies(
												tag -> assertThat(tag.getKey())
														.isEqualTo(SolaceMessageMeterBinder.TAG_NAME),
												tag -> assertThat(tag.getValue()).isEqualTo(nameTagValue)
										)
						),
				m -> assertThat(m.measure())
						.as("Checking measurements for meter %s", meter)
						.filteredOn(measurement -> measurement.getStatistic().equals(Statistic.TOTAL))
						.first()
						.extracting(Measurement::getValue)
						.asInstanceOf(DOUBLE)
						.isEqualTo(value)
		);
	}

	public static ThrowingConsumer<BindingsHealthContributor> isSingleBindingHealthAvailable(String bindingName, int concurrency, Status status) {
		return bindingsHealthContributor -> assertThat(StreamSupport.stream(bindingsHealthContributor.spliterator(), false))
				.singleElement()
				.satisfies(bindingContrib -> assertThat(bindingContrib.getName()).isEqualTo(bindingName))

				.extracting(NamedContributor::getContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isBindingHealthAvailable(concurrency, status));
	}

	public static ThrowingConsumer<BindingHealthContributor> isBindingHealthAvailable(int concurrency, Status status) {
		return bindingHealthContributor -> assertThat(StreamSupport.stream(bindingHealthContributor.spliterator(), false))
				.asInstanceOf(InstanceOfAssertFactories.list(NamedContributor.class))
				.singleElement()

				.satisfies(bindingContrib -> assertThat(bindingContrib.getName()).isEqualTo("flows"))
				.extracting(NamedContributor::getContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(FlowsHealthContributor.class))

				.extracting(flowsContrib -> StreamSupport.stream(flowsContrib.spliterator(), false))
				.asInstanceOf(InstanceOfAssertFactories.stream(NamedContributor.class))
				.satisfies(flowsContrib -> assertThat(flowsContrib.stream().map(NamedContributor::getName))
						.containsExactlyElementsOf(IntStream.range(0, concurrency)
								.mapToObj(i -> "flow-" + i).collect(Collectors.toSet())))


				.extracting(NamedContributor::getContributor)
				.asInstanceOf(InstanceOfAssertFactories.list(FlowHealthIndicator.class))
				.extracting(flowIndicator -> flowIndicator.getHealth(false))
				.allSatisfy(health -> assertThat(health.getStatus()).isEqualTo(status));
	}
}
