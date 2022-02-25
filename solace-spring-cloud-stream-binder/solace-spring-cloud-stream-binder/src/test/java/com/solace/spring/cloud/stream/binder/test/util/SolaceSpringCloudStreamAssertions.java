package com.solace.spring.cloud.stream.binder.test.util;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.ThrowingConsumer;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Assertions to validate Spring Cloud Stream Binder for Solace.
 */
public class SolaceSpringCloudStreamAssertions {
	/**
	 * <p>Returns a function to evaluate a message for a header which may be nested in a batched message.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer) satisfies(ThrowingConsumer)}.</p>
	 * @param header header key
	 * @param type header type
	 * @param isBatched is message expected to be a batched message?
	 * @param requirements requirements which the header value must satisfy. See
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer) satisfies(ThrowingConsumer)}.
	 * @param <T> header type
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer)
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
						.containsKey(SolaceBinderHeaders.BATCHED_HEADERS)
						.satisfies(rootHeaders -> assertThat(rootHeaders.get(SolaceBinderHeaders.BATCHED_HEADERS))
								.isNotNull()
								.isInstanceOf(List.class)
								.asList()
								.isNotEmpty()
								.allSatisfy(msgHeaders -> assertThat(msgHeaders).isInstanceOf(Map.class))
								.map(msgHeaders -> {
									@SuppressWarnings("unchecked")
									Map<String, Object> msgHeadersMap = (Map<String, Object>) msgHeaders;
									return msgHeadersMap;
								})
								.allSatisfy(satisfiesHeader));
			} else {
				assertThat(message.getHeaders()).satisfies(satisfiesHeader);
			}
		};
	}

	/**
	 * <p>Returns a function to evaluate that a consumed Solace message is valid.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer) satisfies(ThrowingConsumer)}.</p>
	 * @param consumerProperties consumer properties
	 * @param expectedMessages the messages against which this message will be evaluated against.
	 *                            Should have a size of exactly 1 if this consumer is not in batch mode.
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer)
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
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer)
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
						.containsKey(SolaceBinderHeaders.BATCHED_HEADERS)
						.satisfies(headers -> assertThat(headers.get(SolaceBinderHeaders.BATCHED_HEADERS))
								.isNotNull()
								.isInstanceOf(List.class)
								.asList()
								.hasSize(expectedMessages.length)
								.allSatisfy(msgHeaders -> {
									@SuppressWarnings("unchecked")
									Map<String, Object> msgHeadersMap = (Map<String, Object>) msgHeaders;
									assertThat(msgHeadersMap)
											.doesNotContainKey(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK);
									assertThat(Optional.ofNullable(msgHeadersMap.get(MessageHeaders.CONTENT_TYPE))
											.map(convertToMimeType)
											.orElse(null))
											.isEqualTo(expectedContentType);
								}));

				assertThat(message.getPayload())
						.isInstanceOf(List.class)
						.asList()
						.containsExactly(Arrays.stream(expectedMessages).map(Message::getPayload).toArray());
			} else {
				assertThat(message.getPayload()).isEqualTo(expectedMessages[0].getPayload());
				assertThat(StaticMessageHeaderAccessor.getContentType(message)).isEqualTo(expectedContentType);
			}
		};
	}

	/**
	 * <p>Returns a function which drains and evaluates the messages for the provided error queue name.</p>
	 * <p>Should be used as a parameter of
	 * {@link org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer) satisfies(ThrowingConsumer)}.</p>
	 * @param jcsmpSession JCSMP session
	 * @param expectedMessages expected messages in error queue
	 * @see org.assertj.core.api.AbstractAssert#satisfies(ThrowingConsumer)
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
}
