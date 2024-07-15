package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.MessageGenerator;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SimpleJCSMPEventHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPInterruptedException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.solacesystems.jcsmp.transaction.RollbackException;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.errorQueueHasMessages;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.hasNestedHeader;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.isValidMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Runs all basic Spring Cloud Stream Binder functionality tests
 * inherited by {@link PartitionCapableBinderTests PartitionCapableBinderTests}
 * along with basic tests specific to the Solace Spring Cloud Stream Binder.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@Execution(ExecutionMode.SAME_THREAD) // parent tests define static destinations
public class SolaceBinderBasicIT extends SpringCloudStreamContext {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderBasicIT.class);

	@BeforeEach
	void setUp(JCSMPSession jcsmpSession, SempV2Api sempV2Api) {
		setJcsmpSession(jcsmpSession);
		setSempV2Api(sempV2Api);
	}

	@AfterEach
	void tearDown() {
		super.cleanup();
		close();
	}

	// NOT YET SUPPORTED ---------------------------------
	@Override
	@Test
	@Execution(ExecutionMode.SAME_THREAD)
	@Disabled("Partitioning is not supported")
	public void testPartitionedModuleSpEL(TestInfo testInfo) throws Exception {
		super.testPartitionedModuleSpEL(testInfo);
	}
	// ---------------------------------------------------

	/**
	 * Basically the same as {@link #testSendAndReceive(TestInfo)}. Reimplemented it to test batch consumption as well.
	 *
	 * @see #testSendAndReceive(TestInfo)
	 */
	@CartesianTest(name = "[{index}] channelType={0}, endpointType={1}, numMessages={2}, batchMode={3}, transacted={4}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testReceive(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
			@Values(ints = {1, 256}) int numMessages,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean transacted,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		if (!batchMode && transacted) {
			logger.info("non-batched, transacted consumers not yet supported");
			return;
		}

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setEndpointType(endpointType);
		consumerProperties.getExtension().setBatchMaxSize(numMessages);
		consumerProperties.getExtension().setTransacted(transacted);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0, numMessages)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		binderBindUnbindLatency();

		Iterator<Message<?>> messageIterator = messages.iterator();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, batchMode ? 1 : numMessages,
				() -> messages.forEach(moduleOutputChannel::send),
				msg -> softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties,
						batchMode ? messages : List.of(messageIterator.next()))));

		String vpnName = (String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);

		if (transacted) {
			retryAssert(() -> assertThat(sempV2Api.monitor().getMsgVpnClientTransactedSessions(vpnName,
							(String) getJcsmpSession().getProperty(JCSMPProperties.CLIENT_NAME), null, null, null, null)
					.getData())
					.singleElement()
					.satisfies(
							d -> assertThat(d.getSuccessCount()).isEqualTo(batchMode ? 1 : numMessages),
							d -> assertThat(d.getCommitCount()).isEqualTo(batchMode ? 1 : numMessages),
							d -> assertThat(d.getFailureCount()).isEqualTo(0),
							d -> assertThat(d.getConsumedMsgCount()).isEqualTo(numMessages),
							d -> assertThat(d.getPendingConsumedMsgCount()).isEqualTo(0)
					));
		}

		retryAssert(() -> assertThat(switch (endpointType) {
			case QUEUE -> sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
					.getData();
			case TOPIC_ENDPOINT -> sempV2Api.monitor()
					.getMsgVpnTopicEndpointMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
					.getData();
		}).hasSize(0));

		producerBinding.unbind();
		consumerBinding.unbind();
	}


	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, transacted={2} namedConsumerGroup={3}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testReceiveBad(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean transacted,
			@Values(booleans = {false, true}) boolean namedConsumerGroup,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		if (!batchMode && transacted) {
			logger.info("non-batched, transacted consumers not yet supported");
			return;
		}

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = namedConsumerGroup ? RandomStringUtils.randomAlphanumeric(10) : null;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setTransacted(transacted);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		binderBindUnbindLatency();

		AtomicInteger expectedDeliveryAttempt = new AtomicInteger(1);
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties, messages));
					if (channelType.equals(PollableSource.class)) {
						// Polled consumers don't increment delivery attempt header
						softly.assertThat(StaticMessageHeaderAccessor.getDeliveryAttempt(msg)).satisfiesAnyOf(
								deliveryAttempt -> assertThat(deliveryAttempt).isNull(),
								deliveryAttempt -> assertThat(deliveryAttempt).isNotNull().hasValue(0));
					} else {
						softly.assertThat(StaticMessageHeaderAccessor.getDeliveryAttempt(msg))
								.isNotNull()
								.hasValue(expectedDeliveryAttempt.getAndIncrement());
					}
					callback.run();
					throw new RuntimeException("bad");
				});

		if (transacted) {
			retryAssert(() -> assertThat(sempV2Api.monitor().getMsgVpnClientTransactedSessions(
					(String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							(String) getJcsmpSession().getProperty(JCSMPProperties.CLIENT_NAME),
							null, null, null, null)
					.getData())
					.singleElement()
					.satisfies(
							d -> assertThat(d.getSuccessCount()).isEqualTo(batchMode ? 1 : messages.size()),
							d -> assertThat(d.getCommitCount()).isEqualTo(0),
							d -> assertThat(d.getRollbackCount()).isEqualTo(batchMode ? 1 : messages.size()),
							d -> assertThat(d.getFailureCount()).isEqualTo(0),
							d -> assertThat(d.getConsumedMsgCount()).isEqualTo(0),
							d -> assertThat(d.getPendingConsumedMsgCount()).isEqualTo(messages.size())
					));
		}

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] numMessages={0}, batched={1}, transacted={2} withConfirmCorrelation={3}")
	public void testSend(@Values(ints = {1, 256}) int numMessages,
						 @Values(booleans = {false, true}) boolean batched,
						 @Values(booleans = {false, true}) boolean transacted,
						 @Values(booleans = {false, true}) boolean withConfirmCorrelation,
						 SempV2Api sempV2Api,
						 SoftAssertions softly,
						 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<DirectChannel> consumerInfrastructureUtil = createConsumerInfrastructureUtil(
				DirectChannel.class);

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.setUseNativeEncoding(true);
		producerProperties.getExtension().setTransacted(transacted);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output", producerBindingProperties);
		DirectChannel moduleInputChannel = consumerInfrastructureUtil
				.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, producerProperties);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<DirectChannel> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel,
				consumerProperties);

		List<CorrelationData> correlationDataList = new ArrayList<>();

		List<Message<?>> messages = IntStream.range(0, numMessages)
				.mapToObj(i -> {
					CorrelationData correlationData;
					if (withConfirmCorrelation && !batched) {
						correlationData = new CorrelationData();
						correlationDataList.add(correlationData);
					} else {
						correlationData = null;
					}

					return MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
							.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
							.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
							.build();
				})
				.collect(Collectors.toList());

		binderBindUnbindLatency();

		Iterator<Message<?>> messageIterator = messages.iterator();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, numMessages, () -> {
			if (batched) {
				CorrelationData correlationData;
				if (withConfirmCorrelation) {
					correlationData = new CorrelationData();
					correlationDataList.add(correlationData);
				} else {
					correlationData = null;
				}

				moduleOutputChannel.send(MessageBuilder.withPayload(messages.stream().map(Message::getPayload).toList())
						.setHeader(SolaceBinderHeaders.BATCHED_HEADERS,
								messages.stream().map(Message::getHeaders).toList())
						.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
						.build());
			} else {
				messages.forEach(moduleOutputChannel::send);
			}
		}, msg -> softly.assertThat(msg).satisfies(isValidMessage(DirectChannel.class, consumerProperties,
				messageIterator.next())));

		String vpnName = (String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);

		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
				.getData())
				.hasSize(0));

		if (transacted) {
			String clientName = (String) getJcsmpSession().getProperty(JCSMPProperties.CLIENT_NAME);
			assertThat(sempV2Api.monitor().getMsgVpnClientRxFlows(vpnName, clientName, null, null, null, null)
					.getData())
					.filteredOn(f -> f.getSessionName() != null)
					.singleElement()
					.satisfies(
							f -> assertThat(f.getGuaranteedMsgCount()).isEqualTo(numMessages),
							f -> assertThat(sempV2Api.monitor().getMsgVpnClientTransactedSession(
											vpnName, clientName, f.getSessionName(), null)
									.getData())
									.satisfies(
											s -> assertThat(s.getSuccessCount()).isEqualTo(batched ? 1 : numMessages),
											s -> assertThat(s.getCommitCount()).isEqualTo(batched ? 1 : numMessages),
											s -> assertThat(s.getFailureCount()).isEqualTo(0),
											s -> assertThat(s.getPublishedMsgCount()).isEqualTo(numMessages),
											s -> assertThat(s.getPendingPublishedMsgCount()).isEqualTo(0)
									)
					);
		}

		assertThat(correlationDataList)
				.hasSize(withConfirmCorrelation ? (batched ? 1 : messages.size()) : 0)
				.allSatisfy(c -> assertThat(c.getFuture()).succeedsWithin(1, TimeUnit.MINUTES));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] numMessages={0} batched={1}, transacted={2}")
	@Execution(ExecutionMode.CONCURRENT)
	public void testProducerErrorChannel(
			@Values(ints = {1, 256}) int numMessages,
			@Values(booleans = {false, true}) boolean batched,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination0EC = String.format("%s%s%s%serrors", binder.getBinder().getBinderIdentity(),
				getDestinationNameDelimiter(), destination0, getDestinationNameDelimiter());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setTransacted(transacted);
		producerProperties.setErrorChannelEnabled(true);
		producerProperties.setUseNativeEncoding(true);
		producerProperties.populateBindingName(destination0);

		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output", bindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);

		MessageGenerator.BatchingConfig batchingConfig = new MessageGenerator.BatchingConfig()
				.setEnabled(batched)
				.setNumberOfMessages(numMessages);
		Message<?> message = MessageGenerator.generateMessage(
				i -> UUID.randomUUID().toString().getBytes(),
				i -> Map.of(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE),
				batchingConfig)
				.build();

		final CompletableFuture<Message<?>> bindingSpecificErrorMessage = new CompletableFuture<>();
		logger.info("Subscribing to binding-specific error channel");
		binder.getApplicationContext()
				.getBean(destination0EC, SubscribableChannel.class)
				.subscribe(bindingSpecificErrorMessage::complete);

		final CompletableFuture<Message<?>> globalErrorMessage = new CompletableFuture<>();
		logger.info("Subscribing to global error channel");
		binder.getApplicationContext()
				.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, SubscribableChannel.class)
				.subscribe(globalErrorMessage::complete);

		jcsmpSession.closeSession();

		assertThatThrownBy(() -> moduleOutputChannel.send(message));

		assertThat(bindingSpecificErrorMessage)
				.succeedsWithin(10, TimeUnit.SECONDS)
				.satisfies(m -> assertThat(globalErrorMessage)
						.succeedsWithin(10, TimeUnit.SECONDS)
						.as("Expected error message sent to global and binding specific channels to be the same")
						.isEqualTo(m))
				.asInstanceOf(InstanceOfAssertFactories.type(ErrorMessage.class))
				.satisfies(
						m -> assertThat(m.getOriginalMessage()).isEqualTo(message),
						m -> assertThat(m.getHeaders().get(IntegrationMessageHeaderAccessor.SOURCE_DATA))
								.satisfies(d -> {
									if (batched) {
										assertThat(d)
												.asInstanceOf(InstanceOfAssertFactories.list(XMLMessage.class))
												.hasSize(batchingConfig.getNumberOfMessages());
									} else {
										assertThat(d).isInstanceOf(XMLMessage.class);
									}
								}))
				.extracting(ErrorMessage::getPayload)
				.asInstanceOf(InstanceOfAssertFactories.throwable(MessagingException.class))
				.cause()
				.isInstanceOf(ClosedFacilityException.class)
				.satisfies(e -> {
					if (transacted) {
						assertThat(e.getSuppressed())
								.singleElement()
								.asInstanceOf(InstanceOfAssertFactories.throwable(ClosedFacilityException.class))
								.hasMessageContaining("Operation ROLLBACK disallowed");
					} else {
						assertThat(e).hasNoSuppressedExceptions();
					}
				});

		producerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] numberOfMessages={0}")
	@Execution(ExecutionMode.CONCURRENT)
	public void testSendBatchOverTransactionLimit(
			@Values(ints = {257, 300}) int numberOfMessages,
			SempV2Api sempV2Api,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination0EC = String.format("%s%s%s%serrors", binder.getBinder().getBinderIdentity(),
				getDestinationNameDelimiter(), destination0, getDestinationNameDelimiter());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties(testInfo);
		producerProperties.getExtension().setTransacted(true);
		producerProperties.setErrorChannelEnabled(true);
		producerProperties.setUseNativeEncoding(true);
		producerProperties.populateBindingName(destination0);

		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);

		DirectChannel moduleOutputChannel = createBindableChannel("output", bindingProperties);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);

		Message<?> message = MessageGenerator.generateMessage(
				i -> UUID.randomUUID().toString().getBytes(),
				i -> Map.of(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE),
				new MessageGenerator.BatchingConfig().setEnabled(true).setNumberOfMessages(numberOfMessages))
				.build();

		final CompletableFuture<Message<?>> bindingSpecificErrorMessage = new CompletableFuture<>();
		logger.info("Subscribing to binding-specific error channel");
		binder.getApplicationContext()
				.getBean(destination0EC, SubscribableChannel.class)
				.subscribe(bindingSpecificErrorMessage::complete);

		assertThatThrownBy(() -> moduleOutputChannel.send(message));

		assertThat(bindingSpecificErrorMessage)
				.succeedsWithin(10, TimeUnit.SECONDS)
				.asInstanceOf(InstanceOfAssertFactories.type(ErrorMessage.class))
				.satisfies(m -> assertThat(m.getOriginalMessage()).isEqualTo(message))
				.extracting(ErrorMessage::getPayload)
				.asInstanceOf(InstanceOfAssertFactories.throwable(MessagingException.class))
				.cause()
				.isInstanceOf(RollbackException.class);

		String vpnName = (String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		String clientName = (String) getJcsmpSession().getProperty(JCSMPProperties.CLIENT_NAME);
		assertThat(sempV2Api.monitor().getMsgVpnClientRxFlows(vpnName, clientName, null, null, null, null)
				.getData())
				.filteredOn(f -> f.getSessionName() != null)
				.singleElement()
				.satisfies(
						f -> assertThat(f.getGuaranteedMsgCount()).isEqualTo(numberOfMessages),
						f -> assertThat(sempV2Api.monitor().getMsgVpnClientTransactedSession(
										vpnName, clientName, f.getSessionName(), null)
								.getData())
								.satisfies(
										s -> assertThat(s.getSuccessCount()).isEqualTo(0),
										s -> assertThat(s.getFailureCount()).isEqualTo(1),
										s -> assertThat(s.getPublishedMsgCount()).isEqualTo(0),
										s -> assertThat(s.getPendingPublishedMsgCount()).isEqualTo(0)
								)
				);

		producerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, transacted={2}, namedConsumerGroup={3}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testConsumerRequeue(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean transacted,
			@Values(booleans = {false, true}) boolean namedConsumerGroup,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		if (!batchMode && transacted) {
			logger.info("non-batched, transacted consumers not yet supported");
			return;
		}

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = namedConsumerGroup ? RandomStringUtils.randomAlphanumeric(10) : null;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setTransacted(transacted);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		binderBindUnbindLatency();

		final AtomicInteger numRetriesRemaining = new AtomicInteger(consumerProperties.getMaxAttempts());
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, numRetriesRemaining.get() + 1,
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties, messages));
					if (numRetriesRemaining.getAndDecrement() > 0) {
						callback.run();
						throw new RuntimeException("Throwing expected exception!");
					} else {
						logger.info("Received message");
						softly.assertThat(msg).satisfies(hasNestedHeader(SolaceHeaders.REDELIVERED, Boolean.class,
								consumerProperties.isBatchMode(), v -> assertThat(v).isTrue()));
						callback.run();
					}
				});

		if (transacted) {
			retryAssert(() -> assertThat(sempV2Api.monitor().getMsgVpnClientTransactedSessions(
							(String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							(String) getJcsmpSession().getProperty(JCSMPProperties.CLIENT_NAME),
							null, null, null, null)
					.getData())
					.singleElement()
					.satisfies(
							d -> assertThat(d.getSuccessCount()).isEqualTo(batchMode ? 2 : 2L * messages.size()),
							d -> assertThat(d.getCommitCount()).isEqualTo(batchMode ? 1 : messages.size()),
							d -> assertThat(d.getRollbackCount()).isEqualTo(batchMode ? 1 : messages.size()),
							d -> assertThat(d.getFailureCount()).isEqualTo(0),
							d -> assertThat(d.getConsumedMsgCount()).isEqualTo(messages.size()),
							d -> assertThat(d.getPendingConsumedMsgCount()).isEqualTo(0)
					));
		}

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, namedConsumerGroup={2}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testConsumerErrorQueueRepublish(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean namedConsumerGroup,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = namedConsumerGroup ? RandomStringUtils.randomAlphanumeric(10) : null;

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					callback.run();
					throw new RuntimeException("Throwing expected exception!");
				});

		assertThat(binder.getConsumerErrorQueueName(consumerBinding))
				.satisfies(errorQueueHasMessages(jcsmpSession, messages));
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
				.getData())
				.hasSize(0));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testFailProducerProvisioningOnRequiredQueuePropertyChange(JCSMPSession jcsmpSession, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueName(destination0, group0, createProducerProperties(testInfo)));

		logger.info(String.format("Pre-provisioning queue %s with AccessType %s to conflict with defaultAccessType %s",
				queue.getName(), endpointProperties.getAccessType(), defaultAccessType));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		Binding<MessageChannel> producerBinding;
		try {
			ExtendedProducerProperties<SolaceProducerProperties> bindingProducerProperties = createProducerProperties(
					testInfo);
			bindingProducerProperties.setRequiredGroups(group0);
			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, bindingProducerProperties);
			producerBinding.unbind();
			fail("Expected producer provisioning to fail due to queue property change");
		} catch (ProvisioningException e) {
			assertThat(e).hasCauseInstanceOf(PropertyMismatchException.class);
			logger.info(String.format("Successfully threw a %s exception with cause %s",
					ProvisioningException.class.getSimpleName(), PropertyMismatchException.class.getSimpleName()));
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testFailConsumerProvisioningOnQueuePropertyChange(JCSMPSession jcsmpSession) throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueNames(destination0, group0, createConsumerProperties(), false)
				.getConsumerGroupQueueName());

		logger.info(String.format("Pre-provisioning queue %s with AccessType %s to conflict with defaultAccessType %s",
				queue.getName(), endpointProperties.getAccessType(), defaultAccessType));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());
		Binding<MessageChannel> consumerBinding;
		try {
			consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, createConsumerProperties());
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to queue property change");
		} catch (ProvisioningException e) {
			assertThat(e).hasCauseInstanceOf(PropertyMismatchException.class);
			logger.info(String.format("Successfully threw a %s exception with cause %s",
					ProvisioningException.class.getSimpleName(), PropertyMismatchException.class.getSimpleName()));
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testFailConsumerProvisioningOnErrorQueuePropertyChange(JCSMPSession jcsmpSession) throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueNames(destination0, group0, createConsumerProperties(), false)
				.getErrorQueueName());

		logger.info(String.format("Pre-provisioning error queue %s with AccessType %s to conflict with defaultAccessType %s",
				errorQueue.getName(), endpointProperties.getAccessType(), defaultAccessType));
		jcsmpSession.provision(errorQueue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());
		Binding<MessageChannel> consumerBinding;
		try {
			ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoBindErrorQueue(true);
			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to error queue property change");
		} catch (ProvisioningException e) {
			assertThat(e).hasCauseInstanceOf(PropertyMismatchException.class);
			logger.info(String.format("Successfully threw a %s exception with cause %s",
					ProvisioningException.class.getSimpleName(), PropertyMismatchException.class.getSimpleName()));
		} finally {
			jcsmpSession.deprovision(errorQueue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testConsumerAdditionalSubscriptions(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel0 = createBindableChannel("output0", new BindingProperties());
		DirectChannel moduleOutputChannel1 = createBindableChannel("output1", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination1 = "some-destl";
		String wildcardDestination1 = destination1.substring(0, destination1.length() - 1) + "*";

		Binding<MessageChannel> producerBinding0 = binder.bindProducer(
				destination0, moduleOutputChannel0, createProducerProperties(testInfo));
		Binding<MessageChannel> producerBinding1 = binder.bindProducer(
				destination1, moduleOutputChannel1, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension()
				.setQueueAdditionalSubscriptions(new String[]{wildcardDestination1, "some-random-sub"});

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(2);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel0.send(message);

		logger.info(String.format("Sending message to destination %s: %s", destination1, message));
		moduleOutputChannel1.send(message);

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		producerBinding0.unbind();
		producerBinding1.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testProducerAdditionalSubscriptions(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel0 = createBindableChannel("output0", new BindingProperties());
		DirectChannel moduleOutputChannel1 = createBindableChannel("output1", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination1 = "some-destl";
		String wildcardDestination1 = destination1.substring(0, destination1.length() - 1) + "*";
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties(testInfo);
		Map<String, String[]> groupsAdditionalSubs = new HashMap<>();
		groupsAdditionalSubs.put(group0, new String[]{wildcardDestination1});
		producerProperties.setRequiredGroups(group0);
		producerProperties.getExtension().setQueueAdditionalSubscriptions(groupsAdditionalSubs);

		Binding<MessageChannel> producerBinding0 = binder.bindProducer(
				destination0, moduleOutputChannel0, producerProperties);
		Binding<MessageChannel> producerBinding1 = binder.bindProducer(
				destination1, moduleOutputChannel1, createProducerProperties(testInfo));

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(2);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel0.send(message);

		logger.info(String.format("Sending message to destination %s: %s", destination1, message));
		moduleOutputChannel1.send(message);

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		producerBinding0.unbind();
		producerBinding1.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1} transacted={2}")
	@Execution(ExecutionMode.SAME_THREAD)
	public <T> void testConsumerReconnect(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean transacted,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			@ExecSvc(scheduled = true, poolSize = 5) ScheduledExecutorService executor,
			TestInfo testInfo) throws Exception {
		if (!batchMode && transacted) {
			logger.info("non-batched, transacted consumers not yet supported");
			return;
		}

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setTransacted(transacted);
		if (consumerProperties.isBatchMode()) {
			// Batch messaging needs a timeout to drain incomplete batches when egress is disabled
			consumerProperties.getExtension().setBatchTimeout((int) TimeUnit.SECONDS.toMillis(20));
		} else {
			consumerProperties.getExtension().setBatchMaxSize(1);
		}

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		binderBindUnbindLatency();

		String vpnName = (String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		String queue0 = binder.getConsumerQueueName(consumerBinding);

		// Minimize message pre-fetch since we're not testing JCSMP, and this influences the test counters
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue()
				.maxDeliveredUnackedMsgsPerFlow((long) consumerProperties.getExtension().getBatchMaxSize()), null, null);
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData()
				.getMaxDeliveredUnackedMsgsPerFlow())
				.isEqualTo(consumerProperties.getExtension().getBatchMaxSize()));

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		consumerInfrastructureUtil.subscribe(moduleInputChannel, executor, message1 -> {
			@SuppressWarnings("unchecked")
			List<byte[]> payloads = consumerProperties.isBatchMode() ? (List<byte[]>) message1.getPayload() :
					Collections.singletonList((byte[]) message1.getPayload());
			for (byte[] payload : payloads) {
				numMsgsConsumed.incrementAndGet();
				logger.trace(String.format("Received message %s", new String(payload)));
				uniquePayloadsReceived.add(new String(payload));
			}
		});

		AtomicBoolean producerStop = new AtomicBoolean(false);
		Future<Integer> producerFuture = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!producerStop.get() && !Thread.currentThread().isInterrupted()) {
				String payload = "foo-" + numMsgsSent;
				Message<?> message = MessageBuilder.withPayload(payload.getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build();
				logger.trace(String.format("Sending message %s", payload));
				try {
					moduleOutputChannel.send(message);
					numMsgsSent += 1;
				} catch (MessagingException e) {
					if (e.getCause() instanceof JCSMPInterruptedException) {
						logger.warn("Received interrupt exception during message produce");
						break;
					} else {
						throw e;
					}
				}
			}
			return numMsgsSent;
		});

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Disabling egress to queue %s", queue0));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().egressEnabled(false), null, null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Enabling egress to queue %s", queue0));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().egressEnabled(true), null, null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Stopping producer");
		producerStop.set(true);
		int numMsgsSent = producerFuture.get(5, TimeUnit.SECONDS);

		softly.assertThat(queue0).satisfies(q -> retryAssert(1, TimeUnit.MINUTES, () ->
				assertThat(sempV2Api.monitor()
						.getMsgVpnQueueMsgs(vpnName, q, Integer.MAX_VALUE, null, null, null)
						.getData()
						.size())
						.as("Expected queue %s to be empty after rebind", q)
						.isEqualTo(0)));

		MonitorMsgVpnQueue queueState = sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData();

		softly.assertThat(queueState.getDisabledBindFailureCount()).isGreaterThan(0);
		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		softly.assertThat(numMsgsConsumed.get()).isGreaterThanOrEqualTo(numMsgsSent);

		logger.info("num-sent: {}, num-consumed: {}, num-redelivered: {}", numMsgsSent, numMsgsConsumed.get(),
				queueState.getRedeliveredMsgCount());
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, transacted={2}")
	@Execution(ExecutionMode.SAME_THREAD)
	public <T> void testConsumerRebind(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(booleans = {false, true}) boolean transacted,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			@ExecSvc(scheduled = true, poolSize = 5) ScheduledExecutorService executor,
			TestInfo testInfo) throws Exception {
		if (!batchMode && transacted) {
			logger.info("non-batched, transacted consumers not yet supported");
			return;
		}

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setTransacted(transacted);
		if (consumerProperties.isBatchMode()) {
			// Batch messaging needs a timeout to drain incomplete batches when egress is disabled
			consumerProperties.getExtension().setBatchTimeout((int) TimeUnit.SECONDS.toMillis(20));
		} else {
			consumerProperties.getExtension().setBatchMaxSize(1);
		}

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		binderBindUnbindLatency();

		String vpnName = (String) getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		String queue0 = binder.getConsumerQueueName(consumerBinding);

		// Minimize message pre-fetch since we're not testing JCSMP, and this influences the test counters
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue()
				.maxDeliveredUnackedMsgsPerFlow((long) consumerProperties.getExtension().getBatchMaxSize()), null, null);
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData()
				.getMaxDeliveredUnackedMsgsPerFlow())
				.isEqualTo(consumerProperties.getExtension().getBatchMaxSize()));

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		consumerInfrastructureUtil.subscribe(moduleInputChannel, executor, message1 -> {
			@SuppressWarnings("unchecked")
			List<byte[]> payloads = consumerProperties.isBatchMode() ? (List<byte[]>) message1.getPayload() :
					Collections.singletonList((byte[]) message1.getPayload());
			for (byte[] payload : payloads) {
				numMsgsConsumed.incrementAndGet();
				logger.trace(String.format("Received message %s", new String(payload)));
				uniquePayloadsReceived.add(new String(payload));
			}
		});

		AtomicBoolean producerStop = new AtomicBoolean(false);
		Future<Integer> producerFuture = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!producerStop.get() && !Thread.currentThread().isInterrupted()) {
				String payload = "foo-" + numMsgsSent;
				Message<?> message = MessageBuilder.withPayload(payload.getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build();
				logger.trace(String.format("Sending message %s", payload));
				try {
					moduleOutputChannel.send(message);
					numMsgsSent += 1;
				} catch (MessagingException e) {
					if (e.getCause() instanceof JCSMPInterruptedException) {
						logger.warn("Received interrupt exception during message produce");
						break;
					} else {
						throw e;
					}
				}
			}
			return numMsgsSent;
		});

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Unbinding consumer");
		consumerBinding.unbind();

		logger.info("Stopping producer");
		producerStop.set(true);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Rebinding consumer");
		consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0, moduleInputChannel,
				consumerProperties);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		int numMsgsSent = producerFuture.get(5, TimeUnit.SECONDS);

		softly.assertThat(queue0).satisfies(q -> retryAssert(1, TimeUnit.MINUTES, () ->
				assertThat(sempV2Api.monitor()
						.getMsgVpnQueueMsgs(vpnName, queue0, Integer.MAX_VALUE, null, null, null)
						.getData()
						.size())
						.as("Expected queue %s to be empty after rebind", queue0)
						.isEqualTo(0)));

		long redeliveredMsgs = sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData()
				.getRedeliveredMsgCount();

		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		// Give margin of error. Redelivered messages might be untracked if it's consumer was shutdown before it could
		// be added to the consumed msg count.
		softly.assertThat(numMsgsConsumed.get() - redeliveredMsgs)
				.isBetween((long) numMsgsSent - consumerProperties.getExtension().getBatchMaxSize(), (long) numMsgsSent);

		logger.info("num-sent: {}, num-consumed: {}, num-redelivered: {}", numMsgsSent, numMsgsConsumed.get(),
				redeliveredMsgs);
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] batchMode={0}")
	public void testBatchTimeoutHasPrecedenceOverPolledConsumerWaitTime(
			@Values(booleans = {false, true}) boolean batchMode) throws Exception {
		SolaceTestBinder binder = getBinder();

		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input",
				new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setBatchTimeout((int) TimeUnit.SECONDS.toMillis(10));
		consumerProperties.getExtension().setPolledConsumerWaitTimeInMillis((int) TimeUnit.SECONDS.toMillis(1));

		assertThat(consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis())
				.as("polled-consumer-wait-time should be at least 1 second for this test")
				.isGreaterThanOrEqualTo((int) TimeUnit.SECONDS.toMillis(1));

		assertThat(consumerProperties.getExtension().getBatchTimeout())
				.as("Batch timeout needs to be at least 10 times larger than polled-consumer-wait-time")
				.isGreaterThanOrEqualTo(10 * consumerProperties.getExtension().getPolledConsumerWaitTimeInMillis());

		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		binderBindUnbindLatency();

		Instant start = Instant.now();
		assertThat(moduleInputChannel.poll(m -> {
		})).isFalse();
		Duration duration = Duration.between(start, Instant.now()).abs();
		if (batchMode) {
			assertThat(duration)
					.isGreaterThanOrEqualTo(Duration.ofMillis(consumerProperties.getExtension().getBatchTimeout()));
		} else {
			assertThat(duration)
					.isGreaterThanOrEqualTo(Duration.ofMillis(consumerProperties.getExtension()
							.getPolledConsumerWaitTimeInMillis()))
					.isLessThan(Duration.ofMillis(consumerProperties.getExtension().getBatchTimeout()).dividedBy(2));
		}

		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testRequestReplyWithRequestor(JCSMPSession jcsmpSession, SoftAssertions softly,
											  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String requestDestination = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer("willBeOverridden", moduleOutputChannel,
				createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(requestDestination,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		final String PROCESSED_SUFFIX = "_PROCESSED";
		String expectedCorrelationId = "theCorrelationId";

		//Prepare the Replier
		moduleInputChannel.subscribe(request -> {
			String reqCorrelationId = request.getHeaders().get(SolaceHeaders.CORRELATION_ID, String.class);
			Destination reqReplyTo = request.getHeaders().get(SolaceHeaders.REPLY_TO, Destination.class);
			String reqPayload = (String) request.getPayload();

			logger.info(String.format("Received request with correlationId [ %s ], replyTo [ %s ], payload [ %s ]",
					reqCorrelationId, reqReplyTo, reqPayload));

			softly.assertThat(reqCorrelationId).isEqualTo(expectedCorrelationId);

			//Send reply message
			Message<?> springMessage = MessageBuilder
					.withPayload(reqPayload + PROCESSED_SUFFIX)
					.setHeader(SolaceHeaders.IS_REPLY, true)
					.setHeader(SolaceHeaders.CORRELATION_ID, reqCorrelationId)
					.setHeader(BinderHeaders.TARGET_DESTINATION, reqReplyTo != null ? reqReplyTo.getName() : "")
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();
			moduleOutputChannel.send(springMessage);
		});

		//Prepare the Requestor (need a producer and a consumer)
		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new SimpleJCSMPEventHandler());
		XMLMessageConsumer consumer = jcsmpSession.getMessageConsumer((XMLMessageListener) null);
		consumer.start();

		TextMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		String requestPayload = "This is the request";
		requestMsg.setText(requestPayload);
		requestMsg.setCorrelationId(expectedCorrelationId);

		//Send request and await for a reply
		Requestor requestor = jcsmpSession.createRequestor();
		BytesXMLMessage replyMsg = requestor.request(requestMsg, 10000, JCSMPFactory.onlyInstance().createTopic(requestDestination));
		assertNotNull(replyMsg, "Did not receive a reply within allotted time");

		softly.assertThat(replyMsg.getCorrelationId()).isEqualTo(expectedCorrelationId);
		softly.assertThat(replyMsg.isReplyMessage()).isTrue();
		String replyPayload = new String(((BytesMessage) replyMsg).getData());
		softly.assertThat(replyPayload).isEqualTo(requestPayload + PROCESSED_SUFFIX);

		producerBinding.unbind();
		consumerBinding.unbind();
		consumer.close();
		producer.close();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseResume(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		assertEquals(defaultWindowSize, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		consumerBinding.pause();
		assertEquals(0, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		consumerBinding.resume();
		assertEquals(defaultWindowSize, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseBeforeConsumerStart(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setAutoStartup(false);
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		assertThat(consumerBinding.isRunning()).isFalse();
		assertThat(consumerBinding.isPaused()).isFalse();

		consumerBinding.pause();
		assertThat(consumerBinding.isRunning()).isFalse();
		assertThat(consumerBinding.isPaused()).isTrue();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);

		consumerBinding.start();
		assertThat(consumerBinding.isRunning()).isTrue();
		assertThat(consumerBinding.isPaused()).isTrue();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1))
				.hasSize(1)
				.allSatisfy(flow -> assertThat(flow.getWindowSize()).isEqualTo(0));

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseStateIsMaintainedWhenConsumerIsRestarted(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);
		consumerBinding.pause();
		assertEquals(0, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.stop();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		//Newly created flow is started in the pause state
		assertEquals(0, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseOnStoppedConsumer(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.stop();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.pause();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		assertEquals(0, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testResumeOnStoppedConsumer(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.pause();
		consumerBinding.stop();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.resume();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		assertEquals(defaultWindowSize, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseResumeOnStoppedConsumer(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.stop();
		assertThat(getQueueTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.pause(); //Has no effect
		consumerBinding.resume(); //Has no effect
		consumerBinding.start();
		assertEquals(defaultWindowSize, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, transacted={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testFailResumeOnClosedConsumer(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean transacted,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(transacted);
		consumerProperties.setBatchMode(transacted); // Transacted-mode requires batch-mode=true
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.pause();
		assertEquals(0, getQueueTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		getJcsmpSession().closeSession();
		RuntimeException exception = assertThrows(RuntimeException.class, consumerBinding::resume);
		assertThat(exception).hasRootCauseInstanceOf(ClosedFacilityException.class);
		assertThat(consumerBinding.isPaused())
				.as("Failed resume should leave the binding in a paused state")
				.isTrue();

		consumerBinding.unbind();
	}

	@Test
	public void testProducerDestinationTypeSetAsQueue(JCSMPSession jcsmpSession) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		String destination = RandomStringUtils.randomAlphanumeric(15);

		SolaceProducerProperties solaceProducerProperties = new SolaceProducerProperties();
		solaceProducerProperties.setDestinationType(DestinationType.QUEUE);

		Binding<MessageChannel> producerBinding = null;
		FlowReceiver flowReceiver = null;
		try {
			producerBinding = binder.bindProducer(destination, moduleOutputChannel, new ExtendedProducerProperties<>(solaceProducerProperties));

			byte[] payload = RandomStringUtils.randomAlphanumeric(15).getBytes();
			Message<?> message = MessageBuilder.withPayload(payload)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			binderBindUnbindLatency();

			logger.info(String.format("Sending message to message handler: %s", message));
			moduleOutputChannel.send(message);

			flowReceiver = jcsmpSession.createFlow(JCSMPFactory.onlyInstance().createQueue(destination), null, null);
			flowReceiver.start();
			BytesXMLMessage solMsg = flowReceiver.receive(5000);
			assertThat(solMsg)
					.isNotNull()
					.isInstanceOf(BytesMessage.class);
			assertThat(((BytesMessage) solMsg).getData()).isEqualTo(payload);
		} finally {
			if (flowReceiver != null) flowReceiver.close();
			if (producerBinding != null) producerBinding.unbind();
		}
	}

	@Test
	public void testProducerDestinationTypeSetAsTopic(JCSMPSession jcsmpSession) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		String destination = RandomStringUtils.randomAlphanumeric(15);

		SolaceProducerProperties solaceProducerProperties = new SolaceProducerProperties();
		solaceProducerProperties.setDestinationType(DestinationType.TOPIC);

		Binding<MessageChannel> producerBinding = null;
		XMLMessageConsumer consumer = null;
		try {
			producerBinding = binder.bindProducer(destination, moduleOutputChannel, new ExtendedProducerProperties<>(solaceProducerProperties));

			jcsmpSession.addSubscription(JCSMPFactory.onlyInstance().createTopic(destination));
			consumer = jcsmpSession.getMessageConsumer((XMLMessageListener) null);
			consumer.start();

			byte[] payload = RandomStringUtils.randomAlphanumeric(15).getBytes();
			Message<?> message = MessageBuilder.withPayload(payload)
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			logger.info(String.format("Sending message to message handler: %s", message));
			moduleOutputChannel.send(message);

			BytesXMLMessage solMsg = consumer.receive(5000);
			assertThat(solMsg)
					.isNotNull()
					.isInstanceOf(BytesMessage.class);
			assertThat(((BytesMessage) solMsg).getData()).isEqualTo(payload);
		} finally {
			if (consumer != null) consumer.close();
			if (jcsmpSession != null)
				jcsmpSession.removeSubscription(JCSMPFactory.onlyInstance().createTopic(destination));
			if (producerBinding != null) producerBinding.unbind();
		}
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testConsumerHeaderExclusion(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			JCSMPProperties jcsmpProperties,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		List<String> excludedHeaders = List.of("headerKey1", "headerKey2", "headerKey5",
				"solace_expiration", "solace_discardIndication", "solace_redelivered",
				"solace_dmqEligible", "solace_priority");

		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setHeaderExclusions(excludedHeaders);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> {
					MessageBuilder<byte[]> messageBuilder =
							MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
									.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE);
					for (int ind = 0; ind < 10; ind++) {
						messageBuilder.setHeader("headerKey" + ind, UUID.randomUUID().toString());
					}
					return messageBuilder.build();
				}).collect(Collectors.toList());

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
				() -> messages.forEach(msg -> {
					softly.assertThat(msg.getHeaders()).containsKeys("headerKey1", "headerKey2", "headerKey5");
					moduleOutputChannel.send(msg);
				}),
				msg -> {
					softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties, messages));

					MessageHeaders springMessageHeaders;
					if (batchMode) {
						@SuppressWarnings("unchecked")
						Map<String, Object> messageHeaders = (Map<String, Object>) Objects.requireNonNull(
								msg.getHeaders().get(SolaceBinderHeaders.BATCHED_HEADERS, List.class)).get(0);
						springMessageHeaders = new MessageHeaders(messageHeaders);
					} else {
						springMessageHeaders = msg.getHeaders();
					}
					softly.assertThat(springMessageHeaders)
							.doesNotContainKeys(excludedHeaders.toArray(new String[0]));
				}
		);

		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME),
						binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
				.getData())
				.hasSize(0));

		producerBinding.unbind();
		consumerBinding.unbind();
	}


	/**
	 * Tests if selected message is received and other message is not
	 */
	@CartesianTest(name = "[{index}] channelType={0}, endpointType={1} ,batchMode={2}, selector=[{3}]")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testConsumerWithSelectorMismatch(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
			@Values(strings = {"uProperty= 'willBeReceived'"}) String selector,
			JCSMPProperties jcsmpProperties,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setEndpointType(endpointType);
		consumerProperties.getExtension().setSelector(selector);

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> badMessage = MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("uProperty", "willNotBeDispatched")
				.build();

		Message<?> goodMessage = MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader("uProperty", "willBeReceived")
				.build();
		List<Message<?>> messages = List.of(badMessage, goodMessage);

		binderBindUnbindLatency();

		String endpointName = binder.getConsumerQueueName(consumerBinding);
		// except only the good message
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
				() -> messages.forEach(moduleOutputChannel::send),
				// receiving only good message
				msg -> softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties, goodMessage)));

		retryAssert(() -> assertThat(
				switch (endpointType) {
					case QUEUE -> getQueueTxFlows(sempV2Api, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME), endpointName, 1).get(0).getSelector();
					case TOPIC_ENDPOINT -> sempV2Api.monitor().getMsgVpnTopicEndpoint(jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME),
									endpointName, null).getData().getSelector();
				}
		).as("Selector not found for endpoint subscription %s", endpointName)
				// all variants of blank selectors will be defaulted to ""
				.isEqualTo((selector == null || selector.isBlank()) ? "" : selector));

		producerBinding.unbind();
		consumerBinding.unbind();
	}


	/**
	 * Tests if selectors are added to queue or durable topic endpoint when connector subscribes for messages
	 */
	@CartesianTest(name = "[{index}] channelType={0}, endpointType={1} ,batchMode={2}, selector=[{3}]")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testConsumerWithSelector(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(strings = {"", " ", "uProperty= 'selectorTest'"}) String selector,
			JCSMPProperties jcsmpProperties,
			SempV2Api sempV2Api,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setEndpointType(endpointType);
		consumerProperties.getExtension().setSelector(selector);

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.setHeader("uProperty", "selectorTest")
						.build())
				.collect(Collectors.toList());
		binderBindUnbindLatency();

		String endpointName = binder.getConsumerQueueName(consumerBinding);
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
				() -> messages.forEach(moduleOutputChannel::send),
				msg -> softly.assertThat(msg).satisfies(isValidMessage(channelType, consumerProperties, messages)));

		retryAssert(() -> assertThat(
				switch (endpointType) {
					case QUEUE -> getQueueTxFlows(sempV2Api, jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME), endpointName, 1).get(0).getSelector();
					case TOPIC_ENDPOINT -> sempV2Api.monitor().getMsgVpnTopicEndpoint(jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME),
									endpointName, null).getData().getSelector();
				}
		).as("Selector not found for endpoint subscription %s", endpointName)
				// all variants of blank selectors will be defaulted to ""
				.isEqualTo((selector == null || selector.isBlank()) ? "" : selector));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testFailConsumerCreateTransactedNonBatched(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setTransacted(true);

		assertThatThrownBy(() ->
				consumerInfrastructureUtil.createBinding(
						binder,
						RandomStringUtils.randomAlphanumeric(10),
						RandomStringUtils.randomAlphanumeric(10),
						moduleInputChannel,
						consumerProperties))
				.satisfies(e -> {
					if (channelType.equals(DirectChannel.class)) {
						assertThat(e).isInstanceOf(BinderException.class);
					}
				})
				.extracting(ExceptionUtils::getRootCause)
				.asInstanceOf(InstanceOfAssertFactories.throwable(IllegalArgumentException.class))
				.hasMessage("Non-batched, transacted consumers are not supported");
	}

	@CartesianTest(name = "[{index}] channelType={0}")
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testFailConsumerCreateTransactedAutoBindErrorQueue(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setBatchMode(true); // transactions only supported in batch mode
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		consumerProperties.getExtension().setTransacted(true);

		assertThatThrownBy(() ->
				consumerInfrastructureUtil.createBinding(
						binder,
						RandomStringUtils.randomAlphanumeric(10),
						RandomStringUtils.randomAlphanumeric(10),
						moduleInputChannel,
						consumerProperties))
				.satisfies(e -> {
					if (channelType.equals(DirectChannel.class)) {
						assertThat(e).isInstanceOf(BinderException.class);
					}
				})
				.extracting(ExceptionUtils::getRootCause)
				.asInstanceOf(InstanceOfAssertFactories.throwable(IllegalArgumentException.class))
				.hasMessage("transacted consumers do not support error queues");
	}


	private List<MonitorMsgVpnQueueTxFlow> getQueueTxFlows(SempV2Api sempV2Api, String vpnName, String queueName, Integer count) throws ApiException {
		return sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, count, null, null, null)
				.getData();
	}

}
