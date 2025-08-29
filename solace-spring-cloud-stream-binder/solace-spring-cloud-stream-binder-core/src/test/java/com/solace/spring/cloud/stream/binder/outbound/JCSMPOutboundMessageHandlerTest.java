package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.spring.MessageGenerator;
import com.solace.spring.cloud.stream.binder.test.spring.MessageGenerator.BatchingConfig;
import com.solace.spring.cloud.stream.binder.test.util.SerializableFoo;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.ErrorChannelSendingCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageConversionException;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import com.solacesystems.jcsmp.transaction.TransactedSession;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

@Timeout(value = 10)
@ExtendWith(MockitoExtension.class)
public class JCSMPOutboundMessageHandlerTest {

	private JCSMPOutboundMessageHandler messageHandler;
	private ArgumentCaptor<JCSMPStreamingPublishCorrelatingEventHandler> pubEventHandlerCaptor;
	private ArgumentCaptor<XMLMessage> xmlMessageCaptor;
	private ArgumentCaptor<Destination> destinationCaptor;
	private ArgumentCaptor<ProducerFlowProperties> producerFlowPropertiesCaptor;
	private ExtendedProducerProperties<SolaceProducerProperties> producerProperties;
	private JCSMPSessionProducerManager sessionProducerManager;
	@Mock private JCSMPSession session;
	@Mock private TransactedSession transactedSession;
	@Mock private XMLMessageProducer messageProducer;
	@Mock private SolaceMeterAccessor solaceMeterAccessor;

	@BeforeEach
	public void init(@Mock MessageChannel errChannel,
					 @Mock SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy,
					 @Mock XMLMessageProducer defaultGlobalSessionProducer) throws JCSMPException {
		xmlMessageCaptor = ArgumentCaptor.forClass(XMLMessage.class);
		destinationCaptor = ArgumentCaptor.forClass(Destination.class);

		Mockito.lenient().when(session.createTransactedSession()).thenReturn(transactedSession);

		producerFlowPropertiesCaptor = ArgumentCaptor.forClass(ProducerFlowProperties.class);
		pubEventHandlerCaptor = ArgumentCaptor.forClass(JCSMPStreamingPublishCorrelatingEventHandler.class);
		Mockito.lenient().when(session.createProducer(
				producerFlowPropertiesCaptor.capture(), pubEventHandlerCaptor.capture()))
				.thenReturn(messageProducer);
		Mockito.lenient().when(transactedSession.createProducer(
						producerFlowPropertiesCaptor.capture(), pubEventHandlerCaptor.capture()))
				.thenReturn(messageProducer);

		Mockito.lenient().when(session.getMessageProducer(Mockito.any())).thenReturn(defaultGlobalSessionProducer);

		ProducerDestination dest = Mockito.mock(ProducerDestination.class);
		Mockito.when(dest.getName()).thenReturn("fake/topic");

		producerProperties = new ExtendedProducerProperties<>(new SolaceProducerProperties());
		producerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(100));

		sessionProducerManager = Mockito.spy(new JCSMPSessionProducerManager(session));

		messageHandler = new JCSMPOutboundMessageHandler(
				dest,
				session,
				errChannel,
				sessionProducerManager,
				producerProperties,
				solaceMeterAccessor
		);
		messageHandler.setErrorMessageStrategy(errorMessageStrategy);
	}

	@CartesianTest(name = "[{index}] transacted={0}")
	public void test_start(@Values(booleans = {false, true}) boolean transacted) throws Exception {
		producerProperties.getExtension().setTransacted(transacted);
		messageHandler.start();

		if (transacted) {
			Mockito.verify(session, Mockito.never()).createProducer(Mockito.any(), Mockito.any());
			Mockito.verify(transactedSession).createProducer(Mockito.any(), Mockito.any());
		} else {
			Mockito.verify(session).createProducer(Mockito.any(), Mockito.any());
			Mockito.verify(transactedSession, Mockito.never()).createProducer(Mockito.any(), Mockito.any());
		}
	}

	@CartesianTest(name = "[{index}] transacted={0}")
	public void test_start_fail(@Values(booleans = {false, true}) boolean transacted) throws Exception {
		producerProperties.getExtension().setTransacted(transacted);

		JCSMPException exception = new JCSMPException("error");
		if (transacted) {
			Mockito.doThrow(exception).when(transactedSession).createProducer(Mockito.any(), Mockito.any());
		} else {
			Mockito.doThrow(exception).when(session).createProducer(Mockito.any(), Mockito.any());
		}

		assertThatThrownBy(() -> messageHandler.start()).hasRootCause(exception);
		Mockito.verify(sessionProducerManager).release(Mockito.any());
		if (transacted) {
			Mockito.verify(transactedSession).close();
		}
	}

	@CartesianTest(name = "[{index}] batched={0} transacted={1} payloadType={2}")
	public void test_responseReceived_withInTimeout(
			@Values(booleans = {false, true}) boolean batched,
			@Values(booleans = {false, true}) boolean transacted,
			@Values(classes = {String.class, List.class}) Class<?> payloadType) throws Exception {
		producerProperties.getExtension().setTransacted(transacted);
		messageHandler.start();

		CorrelationData correlationData = new CorrelationData();
		BatchingConfig batchingConfig = new BatchingConfig().setEnabled(batched);
		messageHandler.handleMessage(MessageGenerator.generateMessage(
						i -> {
							if (payloadType.equals(List.class)) {
								return List.of("test-0", "test-1", "test-2");
							} else if (payloadType.equals(String.class)) {
								return "test";
							} else {
								throw new IllegalArgumentException("No test for payload type " + payloadType);
							}
						},
						i -> Map.of(),
						batchingConfig)
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build());

		AtomicInteger timesSuccessResolved = new AtomicInteger(0);
		AtomicInteger timesFailureResolved = new AtomicInteger(0);
		correlationData.getFuture().addCallback(
				v -> timesSuccessResolved.incrementAndGet(),
				e -> timesFailureResolved.incrementAndGet());

		if (transacted) {
			Mockito.verify(transactedSession).commit();
		}

		getCorrelationKeys().forEach(pubEventHandlerCaptor.getValue()::responseReceivedEx);
		assertThat(xmlMessageCaptor.getAllValues())
				.hasSize(batched ? batchingConfig.getNumberOfMessages() : 1)
				.satisfies(msgs -> {
					boolean lastMsgIsAckImmediately = batched && !transacted;
					assertThat(lastMsgIsAckImmediately ? msgs.subList(0, msgs.size() - 1) : msgs)
							.extracting(XMLMessage::isAckImmediately)
							.containsOnly(false);

					if (lastMsgIsAckImmediately) {
						assertThat(msgs)
								.last()
								.extracting(XMLMessage::isAckImmediately)
								.isEqualTo(true);
					}
				});

		assertThat(correlationData.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);
		assertThat(timesSuccessResolved).hasValue(1);
		assertThat(timesFailureResolved).hasValue(0);
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void test_handleError_withInTimeout(@Values(booleans = {false, true}) boolean batched) throws Exception {
		messageHandler.start();

		CorrelationData correlationData = new CorrelationData();
		messageHandler.handleMessage(MessageGenerator.generateMessage(
						i -> RandomStringUtils.randomAlphanumeric(100),
						i -> Map.of(),
						new BatchingConfig().setEnabled(batched))
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build());

		AtomicInteger timesSuccessResolved = new AtomicInteger(0);
		AtomicInteger timesFailureResolved = new AtomicInteger(0);
		correlationData.getFuture().addCallback(
				v -> timesSuccessResolved.incrementAndGet(),
				e -> timesFailureResolved.incrementAndGet());

		JCSMPException exception = new JCSMPException("ooooops");
		getCorrelationKeys().forEach(k -> pubEventHandlerCaptor.getValue()
				.handleErrorEx(k, exception, 1111));

		assertThat(correlationData.getFuture())
				.failsWithin(100, TimeUnit.MILLISECONDS)
				.withThrowableOfType(ExecutionException.class)
				.havingCause()
				.isInstanceOf(MessagingException.class)
				.withCause(exception);

		assertThat(timesSuccessResolved).hasValue(0);
		assertThat(timesFailureResolved).hasValue(1);
	}

	@Test
	public void test_handleError_middleOfBatch() throws Exception {
		messageHandler.start();

		CorrelationData correlationData = new CorrelationData();
		messageHandler.handleMessage(MessageGenerator.generateMessage(
						i -> RandomStringUtils.randomAlphanumeric(100),
						i -> Map.of(),
						new BatchingConfig().setEnabled(true).setNumberOfMessages(10))
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build());

		AtomicInteger timesSuccessResolved = new AtomicInteger(0);
		AtomicInteger timesFailureResolved = new AtomicInteger(0);
		correlationData.getFuture().addCallback(
				v -> timesSuccessResolved.incrementAndGet(),
				e -> timesFailureResolved.incrementAndGet());


		List<Object> correlationKeys = getCorrelationKeys();
		JCSMPStreamingPublishCorrelatingEventHandler pubEventHandler = pubEventHandlerCaptor.getValue();
		for (int i = 0; i < correlationKeys.size(); i++) {
			Object correlationKey = correlationKeys.get(i);
			if (i == (correlationKeys.size() / 2)) {
				pubEventHandler.handleErrorEx(correlationKey, new JCSMPException("ooooops"), 1111);
			} else {
				pubEventHandler.responseReceivedEx(correlationKey);
			}
		}

		assertThatThrownBy(() -> correlationData.getFuture().get(100, TimeUnit.MILLISECONDS))
				.isInstanceOf(ExecutionException.class)
				.cause()
				.isInstanceOf(MessagingException.class)
				.cause()
				.isInstanceOf(JCSMPException.class)
				.hasMessage("ooooops");

		assertThat(timesSuccessResolved).hasValue(0);
		assertThat(timesFailureResolved).hasValue(1);
	}

	@Test()
	public void test_responseReceived_withOutTimeout() {
		messageHandler.start();

		CorrelationData correlationData = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build());

		assertThat(correlationData.getFuture())
				.failsWithin(100, TimeUnit.MILLISECONDS)
				.withThrowableThat()
				.isInstanceOf(TimeoutException.class);
	}

	@Test()
	public void test_responseReceived_raceCondition() {
		messageHandler.start();

		CorrelationData correlationDataA = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationDataA)
				.build());
		CorrelationData correlationDataB = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationDataB)
				.build());
		CorrelationData correlationDataC = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationDataC)
				.build());

		JCSMPStreamingPublishCorrelatingEventHandler pubEventHandler = pubEventHandlerCaptor.getValue();
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataB));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataA));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataC));

		assertThat(correlationDataA.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);
		assertThat(correlationDataB.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);
		assertThat(correlationDataC.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);
	}

	@Test()
	public void test_responseReceived_messageIdCollision_oneAfterTheOther() {
		messageHandler.start();
		JCSMPStreamingPublishCorrelatingEventHandler pubEventHandler = pubEventHandlerCaptor.getValue();

		CorrelationData correlationDataA = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationDataA)
				.build());
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataA));

		assertThat(correlationDataA.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);


		CorrelationData correlationDataB = new CorrelationData();
		messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationDataB)
				.build());
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataB));

		assertThat(correlationDataB.getFuture()).succeedsWithin(100, TimeUnit.MILLISECONDS);
	}

	@ParameterizedTest
	@ValueSource(classes = {JCSMPException.class, RollbackException.class})
	public void test_transactionRollback_onError(Class<JCSMPException> commitError) throws Exception {
		producerProperties.getExtension().setTransacted(true);
		messageHandler.start();

		JCSMPException exception = commitError.getConstructor(String.class).newInstance("test");
		Mockito.doThrow(exception).when(transactedSession).commit();

		CorrelationData correlationData = new CorrelationData();
		assertThatThrownBy(() -> messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build()))
				.isInstanceOf(MessagingException.class)
				.hasRootCause(exception);

		Mockito.verify(transactedSession, Mockito.times(commitError.equals(RollbackException.class) ? 0 : 1)).rollback();
		assertThat(correlationData.getFuture())
				.failsWithin(1, TimeUnit.MINUTES)
				.withThrowableThat()
				.isInstanceOf(ExecutionException.class)
				.havingRootCause()
				.isEqualTo(exception);
	}

	@Test
	public void test_transactionRollbackFailure() throws Exception {
		producerProperties.getExtension().setTransacted(true);
		messageHandler.start();

		JCSMPException commitException = new JCSMPException("commit error");
		Mockito.doThrow(commitException).when(transactedSession).commit();

		JCSMPException rollbackException = new JCSMPException("rollback error");
		Mockito.doThrow(rollbackException).when(transactedSession).rollback();

		assertThatThrownBy(() -> messageHandler.handleMessage(MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, new CorrelationData())
				.build()))
				.isInstanceOf(MessagingException.class)
				.rootCause()
				.isEqualTo(commitException)
				.hasSuppressedException(rollbackException);
		Mockito.verify(transactedSession).rollback();
	}

	@ParameterizedTest(name = "[{index}] success={0}")
	@ValueSource(booleans = {false, true})
	public void testMeter(boolean success) throws Exception {
		messageHandler.start();

		Message<String> message = MessageBuilder.withPayload(RandomStringUtils.randomAlphanumeric(100))
				.build();

		if (success) {
			messageHandler.handleMessage(message);
		} else {
			JCSMPException exception = new JCSMPException("Expected exception");
			Mockito.doThrow(exception)
					.when(messageProducer)
					.send(xmlMessageCaptor.capture(), any(Destination.class));
			assertThatThrownBy(() -> messageHandler.handleMessage(message))
					.isInstanceOf(MessagingException.class)
					.hasCause(exception);
		}

		Mockito.verify(solaceMeterAccessor, Mockito.times(1))
				.recordMessage(Mockito.eq(producerProperties.getBindingName()), any());
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void test_dynamic_destinationName_only(
			@Values(booleans = {false, true}) boolean batched) throws JCSMPException {
		messageHandler.start();

		BatchingConfig batchingConfig = new BatchingConfig().setEnabled(batched);
		List<String> targetDestinations = new ArrayList<>();
		Message<?> message = MessageGenerator.generateMessage(
				i -> RandomStringUtils.randomAlphanumeric(10),
				i -> {
					String targetDestination = RandomStringUtils.randomAlphanumeric(100);
					targetDestinations.add(targetDestination);
					return Map.ofEntries(
							Map.entry(BinderHeaders.TARGET_DESTINATION, targetDestination),
							Map.entry("SOME_HEADER", "HOLA") //add extra header and confirm it is kept
					);
				}, batchingConfig)
				.build();

		messageHandler.handleMessage(message);

		Mockito.verify(messageProducer, Mockito.times(batched ? batchingConfig.getNumberOfMessages() : 1))
				.send(xmlMessageCaptor.capture(), destinationCaptor.capture());

		assertThat(destinationCaptor.getAllValues())
				.asInstanceOf(InstanceOfAssertFactories.list(Topic.class))
				.extracting(Destination::getName)
				.containsExactlyElementsOf(targetDestinations);

		assertThat(xmlMessageCaptor.getAllValues())
				.extracting(XMLMessage::getProperties)
				.allSatisfy(p -> assertThat(p.get(BinderHeaders.TARGET_DESTINATION)).isNull())
				.allSatisfy(p -> assertThat(p.get("SOME_HEADER")).isEqualTo("HOLA"));
	}

	@CartesianTest(name = "[{index}] destinationType={0} batched={1}")
	public void test_dynamic_destinationName_and_destinationType(
			@Values(strings = { "topic", "queue", " TOPIc ", " QueUe  ", "", "   " }) String destinationType,
			@Values(booleans = {false, true}) boolean batched) throws JCSMPException {
		messageHandler.start();

		BatchingConfig batchingConfig = new BatchingConfig().setEnabled(batched);
		List<String> targetDestinations = new ArrayList<>();
		Message<?> message = MessageGenerator.generateMessage(
				i -> RandomStringUtils.randomAlphanumeric(100),
				i -> {
					String targetDestination = RandomStringUtils.randomAlphanumeric(100);
					targetDestinations.add(targetDestination);
					return Map.ofEntries(
							Map.entry(BinderHeaders.TARGET_DESTINATION, targetDestination),
							Map.entry(SolaceBinderHeaders.TARGET_DESTINATION_TYPE, destinationType));
				},
				batchingConfig)
				.build();

		messageHandler.handleMessage(message);

		Mockito.verify(messageProducer, Mockito.times(batched ? batchingConfig.getNumberOfMessages() : 1))
				.send(xmlMessageCaptor.capture(), destinationCaptor.capture());

		//MessageHandler uses default producerProperties so blank and unspecified destinationType defaults to Topic
		assertThat(destinationCaptor.getAllValues())
				.allSatisfy(d -> assertThat(d).isInstanceOf(
						destinationType.trim().equalsIgnoreCase("queue") ? Queue.class : Topic.class))
				.extracting(Destination::getName)
				.containsExactlyElementsOf(targetDestinations);

		//Verify headers don't get set on ongoing Solace message
		assertThat(xmlMessageCaptor.getAllValues())
				.extracting(XMLMessage::getProperties)
				.allSatisfy(p -> assertThat(p.get(BinderHeaders.TARGET_DESTINATION)).isNull())
				.allSatisfy(p -> assertThat(p.get(SolaceBinderHeaders.TARGET_DESTINATION_TYPE)).isNull());
	}

	@CartesianTest(name = "[{index}] type={0} batched={1}")
	public void test_dynamic_destinationName_with_destinationType_configured_on_messageHandler(
			@Values(strings = { "queue", "topic" }) String type,
			@Values(booleans = {false, true}) boolean batched) throws JCSMPException {
		messageHandler.start();

		SolaceProducerProperties producerProperties = new SolaceProducerProperties();
		producerProperties.setDestinationType(type.equals("queue") ? DestinationType.QUEUE : DestinationType.TOPIC);
		ProducerDestination dest = Mockito.mock(ProducerDestination.class);
		Mockito.when(dest.getName()).thenReturn("thisIsOverriddenByDynamicDestinationName");

		messageHandler = new JCSMPOutboundMessageHandler(
				dest,
				session,
				null,
				new JCSMPSessionProducerManager(session),
				new ExtendedProducerProperties<>(producerProperties),
				solaceMeterAccessor
		);
		messageHandler.start();

		BatchingConfig batchingConfig = new BatchingConfig().setEnabled(batched);
		List<String> targetDestinations = new ArrayList<>();
		Message<?> message = MessageGenerator.generateMessage(
						i -> RandomStringUtils.randomAlphanumeric(100),
						i -> {
							String targetDestination = RandomStringUtils.randomAlphanumeric(100);
							targetDestinations.add(targetDestination);
							return Map.of(BinderHeaders.TARGET_DESTINATION, targetDestination);
						},
						batchingConfig)
				.build();

		messageHandler.handleMessage(message);

		Mockito.verify(messageProducer, Mockito.times(batched ? batchingConfig.getNumberOfMessages() : 1))
				.send(any(), destinationCaptor.capture());

		assertThat(destinationCaptor.getAllValues())
				.allSatisfy(d -> assertThat(d).isInstanceOf(type.equals("queue") ? Queue.class : Topic.class))
				.extracting(Destination::getName)
				.containsExactlyElementsOf(targetDestinations);
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void test_dynamic_destination_with_invalid_destinationType(
			@Values(booleans = {false, true}) boolean batched) {
		messageHandler.start();
		Message<?> message = MessageGenerator.generateMessage(
				i -> RandomStringUtils.randomAlphanumeric(100),
				i -> Map.ofEntries(
						Map.entry(BinderHeaders.TARGET_DESTINATION, "dynamicDestinationName"),
						Map.entry(SolaceBinderHeaders.TARGET_DESTINATION_TYPE, "INVALID")
				), new BatchingConfig().setEnabled(batched))
				.build();
		Exception exception = assertThrows(MessagingException.class, () -> messageHandler.handleMessage(message));
		assertThat(exception)
				.hasRootCauseInstanceOf(IllegalArgumentException.class)
				.hasRootCauseMessage("Incorrect value specified for header 'solace_scst_targetDestinationType'. Expected [ TOPIC|QUEUE ] but actual value is [ INVALID ]");
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void test_dynamic_destinationName_with_invalid_header_value_type(
			@Values(booleans = {false, true}) boolean batched) {
		messageHandler.start();
		Message<?> message = MessageGenerator.generateMessage(
						i -> RandomStringUtils.randomAlphanumeric(100),
						i -> Map.of(BinderHeaders.TARGET_DESTINATION, Instant.now()),
						new BatchingConfig().setEnabled(batched))
				.build();
		Exception exception = assertThrows(MessagingException.class, () -> messageHandler.handleMessage(message));
		assertThat(exception)
				.hasRootCauseInstanceOf(IllegalArgumentException.class)
				.hasRootCauseMessage("Incorrect type specified for header 'scst_targetDestination'. Expected [class java.lang.String] but actual type is [class java.time.Instant]");
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void test_dynamic_destinationType_with_invalid_header_value_type(
			@Values(booleans = {false, true}) boolean batched) {
		messageHandler.start();
		Message<?> message = MessageGenerator.generateMessage(
						i -> RandomStringUtils.randomAlphanumeric(100),
						i -> Map.ofEntries(
								Map.entry(BinderHeaders.TARGET_DESTINATION, "someDynamicDestinationName"),
								Map.entry(SolaceBinderHeaders.TARGET_DESTINATION_TYPE, Instant.now())
						), new BatchingConfig().setEnabled(batched))
				.build();
		Exception exception = assertThrows(MessagingException.class, () -> messageHandler.handleMessage(message));
		assertThat(exception)
				.hasRootCauseInstanceOf(IllegalArgumentException.class)
				.hasRootCauseMessage("Incorrect type specified for header 'solace_scst_targetDestinationType'. Expected [class java.lang.String] but actual type is [class java.time.Instant]");
	}

	// Can remove test if/when SOL-118898 is completed
	@CartesianTest(name = "[{index}] pubAckWindowSize={0}, ackEventMode={1}")
	public void testJCSMPPropertiesInheritanceWorkaround(
			@Values(ints = {1, 100, 255}) int pubAckWindowSize,
			@Values(strings = {
					JCSMPProperties.SUPPORTED_ACK_EVENT_MODE_PER_MSG,
					JCSMPProperties.SUPPORTED_ACK_EVENT_MODE_WINDOWED}) String ackEventMode) {
		Mockito.when(session.getProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE)).thenReturn(pubAckWindowSize);
		Mockito.when(session.getProperty(JCSMPProperties.ACK_EVENT_MODE)).thenReturn(ackEventMode);
		messageHandler.start();

		assertThat(producerFlowPropertiesCaptor.getValue())
				.satisfies(
						p -> assertThat(p.getWindowSize()).isEqualTo(pubAckWindowSize),
						p -> assertThat(p.getAckEventMode()).isEqualTo(ackEventMode));
	}

	@CartesianTest(name = "[{index}] batched={0} witSerializablePayload={1} configureBeforeStart={2} payloadTypeCompatibility={3}")
	void test_ModifySmfMessageWriterProperties(
			@Values(booleans = {false, true}) boolean batched,
			@Values(booleans = {false, true}) boolean configureBeforeStart,
			@Values(booleans = {false, true}) boolean witSerializablePayload,
			@CartesianTest.Enum SmfMessagePayloadWriteCompatibility payloadTypeCompatibility)
			throws Exception {
		assertThat(messageHandler.getSmfMessageWriterProperties().getHeaderExclusions())
				.as("Test error: headerExclusions should be empty")
				.isEmpty();
		assertThat(messageHandler.getSmfMessageWriterProperties().getHeaderTypeCompatibility())
				.as("Test error: headerTypeCompatibility should be default")
				.isEqualTo(SmfMessageHeaderWriteCompatibility.SERIALIZE_AND_ENCODE_NON_NATIVE_TYPES);
		assertThat(messageHandler.getSmfMessageWriterProperties().getPayloadTypeCompatibility())
				.as("Test error: payloadTypeCompatibility should be default")
				.isEqualTo(SmfMessagePayloadWriteCompatibility.SERIALIZE_NON_NATIVE_TYPES);

		byte[] bytesPayload = RandomStringUtils.randomAlphanumeric(100).getBytes(StandardCharsets.UTF_8);
		SerializableFoo serializablePayload = new SerializableFoo("foo", "bar");
		BatchingConfig batchingConfig = new BatchingConfig().setEnabled(batched).setNumberOfMessages(10);

		Message<?> messageToSend = MessageGenerator.generateMessage(
						i -> witSerializablePayload ? serializablePayload : bytesPayload,
						i -> Map.of("excluded", "foo"),
						batchingConfig)
				.build();

		if (!configureBeforeStart) {
			messageHandler.start();
		}

		messageHandler.getSmfMessageWriterProperties().setPayloadTypeCompatibility(payloadTypeCompatibility);
		messageHandler.getSmfMessageWriterProperties().getHeaderExclusions().add("excluded");

		if (configureBeforeStart) {
			messageHandler.start();
		}

		if (witSerializablePayload && payloadTypeCompatibility.equals(SmfMessagePayloadWriteCompatibility.NATIVE_ONLY)) {
			assertThatThrownBy(() -> messageHandler.handleMessage(messageToSend))
					.isInstanceOf(SolaceMessageConversionException.class)
					.hasMessageContaining("Invalid payload received")
					.hasMessageContaining("Received: %s", SerializableFoo.class.getName());
			return;
		}

		messageHandler.handleMessage(messageToSend);

		Mockito.verify(messageProducer, Mockito.atLeastOnce()).send(xmlMessageCaptor.capture(), any(Destination.class));

		assertThat(xmlMessageCaptor.getAllValues())
				.hasSize(batched ? batchingConfig.getNumberOfMessages() : 1)
				.allSatisfy(msg -> {
					assertThat(msg)
							.asInstanceOf(InstanceOfAssertFactories.type(BytesMessage.class))
							.extracting(BytesMessage::getData)
							.isEqualTo(witSerializablePayload ? SerializationUtils.serialize(serializablePayload) :
									bytesPayload);
					assertThat(msg.getProperties().containsKey("excluded"))
							.as("Header 'excluded' should be excluded")
							.isFalse();
				});
	}

	@Test
	void testGetBindingName() {
		assertThat(messageHandler.getBindingName()).isNotEmpty().isEqualTo(producerProperties.getBindingName());
	}

	private List<Object> getCorrelationKeys() throws JCSMPException {
		Mockito.verify(messageProducer, Mockito.atLeastOnce()).send(xmlMessageCaptor.capture(), any(Destination.class));
		return xmlMessageCaptor.getAllValues()
				.stream()
				.map(XMLMessage::getCorrelationKey)
				.toList();
	}

	private ErrorChannelSendingCorrelationKey createCorrelationKey(CorrelationData correlationData, Message<?> msg) {
		ErrorChannelSendingCorrelationKey key = new ErrorChannelSendingCorrelationKey(
				msg,
				Mockito.mock(MessageChannel.class),
				new SolaceMessageHeaderErrorMessageStrategy());
		key.setConfirmCorrelation(correlationData);
		return key;
	}

	private ErrorChannelSendingCorrelationKey createCorrelationKey(CorrelationData correlationData) {
		Message<String> msg = MessageBuilder.withPayload("the empty payload")
				.build();
		return createCorrelationKey(correlationData, msg);
	}

	@CartesianTest(name = "[{index}] transacted={0}")
	public void test_startFailedForBadHeaderNameMapping(@Values(booleans = {false, true}) boolean transacted) {
		producerProperties.getExtension().setTransacted(transacted);
		producerProperties.getExtension().setHeaderNameMapping(Map.of("k1", "v1", "k2", "v1"));

		assertThatThrownBy(() -> messageHandler.start())
				.hasRootCauseInstanceOf(MessagingException.class)
				.extracting(throwable -> throwable.getCause().getMessage()).asString()
				.startsWith("Two or more headers map to the same header name in headerNameMapping");
	}
}