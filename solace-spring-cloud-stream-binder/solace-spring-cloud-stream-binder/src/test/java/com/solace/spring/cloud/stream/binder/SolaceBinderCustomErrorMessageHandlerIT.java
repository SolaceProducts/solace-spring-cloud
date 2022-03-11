package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.RequeueCurrentMessageException;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.errorQueueHasMessages;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.hasNestedHeader;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.isValidConsumerErrorMessage;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.isValidProducerErrorMessage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * All tests regarding custom channel-specific error message handlers
 * (i.e. overriding {@link SolaceErrorMessageHandler}).
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderCustomErrorMessageHandlerIT {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderCustomErrorMessageHandlerIT.class);

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, maxAttempts={2}")
	public <T> void testConsumerOverrideErrorMessageHandler(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(ints = {1, 3}) int maxAttempts,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			SoftAssertions softly,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + group0 +
				context.getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.setMaxAttempts(maxAttempts);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		// Need to create channel before so that the override actually works
		final CountDownLatch errorLatch = new CountDownLatch(1);
		context.createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: {}", StaticMessageHeaderAccessor.getId(msg));
			softly.assertThat(msg).satisfies(isValidConsumerErrorMessage(consumerProperties,
					channelType.isAssignableFrom(PollableSource.class), true, messages));
			errorLatch.countDown();
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					logger.info("Received message {}", StaticMessageHeaderAccessor.getId(msg));
					callback.run();
					throw new RuntimeException("bad");
				});
		assertThat(errorLatch.await(10, TimeUnit.SECONDS)).isTrue();
		softly.assertAll();

		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(0);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, maxAttempts={2}")
	public <T> void testConsumerOverrideErrorMessageHandlerThrowException(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(ints = {1, 3}) int maxAttempts,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + group0 +
				context.getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		context.createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: {}", StaticMessageHeaderAccessor.getId(msg));
			throw new ConsumerInfrastructureUtil.ExpectedMessageHandlerException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.setMaxAttempts(maxAttempts);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					logger.info("Received message {}", StaticMessageHeaderAccessor.getId(msg));
					callback.run();
					throw new RuntimeException("bad");
				});

		assertThat(binder.getConsumerErrorQueueName(consumerBinding))
				.satisfies(errorQueueHasMessages(jcsmpSession, messages));

		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(0);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, maxAttempts={2}")
	public <T> void testConsumerOverrideErrorMessageHandlerThrowRequeueException(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Values(ints = {1, 3}) int maxAttempts,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + group0 +
				context.getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		context.createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: {}", StaticMessageHeaderAccessor.getId(msg));
			throw new RequeueCurrentMessageException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.setMaxAttempts(maxAttempts);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList());

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts() + 1,
				() -> messages.forEach(moduleOutputChannel::send),
				(msg, callback) -> {
					logger.info("Received message {}", StaticMessageHeaderAccessor.getId(msg));
					if (hasNestedBooleanHeader(SolaceHeaders.REDELIVERED, msg, consumerProperties.isBatchMode())) {
						callback.run();
					} else {
						callback.run();
						throw new RuntimeException("bad");
					}
				});

		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(messages.size());
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
	public <T> void testConsumerOverrideErrorMessageHandlerThrowExceptionAndStale(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {false, true}) boolean batchMode,
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			SoftAssertions softly,
			@ExecSvc(scheduled = true, poolSize = 1) ScheduledExecutorService executorService,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + group0 +
				context.getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		CountDownLatch continueLatch = new CountDownLatch(1);
		CountDownLatch errorStartLatch = new CountDownLatch(1);
		context.createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: {}", StaticMessageHeaderAccessor.getId(msg));
			errorStartLatch.countDown();
			try {
				softly.assertThat(continueLatch.await(1, TimeUnit.MINUTES)).isTrue();
			} catch (InterruptedException e) {
				softly.fail("interrupted while waiting for continue latch", e);
				throw new RuntimeException(e);
			}
			throw new RuntimeException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.setMaxAttempts(1);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		consumerProperties.getExtension().setFlowPreRebindWaitTimeout(0);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		List<Message<?>> messages = IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.setHeader("skip", true)
						.build())
				.collect(Collectors.toList());
		messages.addAll(IntStream.range(0,
						batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
				.mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build())
				.collect(Collectors.toList()));

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		assertThat(sempV2Api.monitor().getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getMaxDeliveredUnackedMsgsPerFlow())
				.as("queue %s does not have a enough flow capacity to run this test", queueName)
				.isGreaterThanOrEqualTo(messages.size());

		CompletableFuture<AcknowledgmentCallback> staleTriggeringAckFuture = new CompletableFuture<>();

		consumerInfrastructureUtil.subscribe(moduleInputChannel, executorService, msg -> {
			logger.info("Received message {}", StaticMessageHeaderAccessor.getId(msg));
			if (!hasNestedBooleanHeader(SolaceHeaders.REDELIVERED, msg, consumerProperties.isBatchMode())) {
				if (hasNestedBooleanHeader("skip", msg, consumerProperties.isBatchMode())) {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					softly.assertThat(ackCallback).isNotNull();
					ackCallback.noAutoAck();
					staleTriggeringAckFuture.complete(ackCallback);
				} else {
					throw new RuntimeException("bad");
				}
			}
		});

		messages.forEach(moduleOutputChannel::send);

		AcknowledgmentCallback staleTriggeringAck = staleTriggeringAckFuture.get(1, TimeUnit.MINUTES);
		assertThat(errorStartLatch.await(1, TimeUnit.MINUTES)).isTrue();
		AckUtils.requeue(staleTriggeringAck); // Force real message to be stale

		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getBindSuccessCount())
				.isEqualTo(3));

		continueLatch.countDown();
		softly.assertAll();

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(messages.size());
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, errorQueueName, 1, null, null, null)
					.getData())
					.hasSize(0);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testPublisherErrorMessageHandler(SpringCloudStreamContext context, SoftAssertions softly,
												 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + "errors";

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel,
				producerProperties);

		final CountDownLatch errorLatch = new CountDownLatch(1);
		context.createChannel(errorDestination0, PublishSubscribeChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			softly.assertThat(msg).satisfies(isValidProducerErrorMessage(false));
			errorLatch.countDown();
		});

		context.binderBindUnbindLatency();

		assertThrows(MessagingException.class, () -> moduleOutputChannel.send(
				MessageBuilder.withPayload("foo".getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.setHeader(BinderHeaders.TARGET_DESTINATION, new Object()) // force a publish error
						.build()));
		assertThat(errorLatch.await(10, TimeUnit.SECONDS)).isTrue();

		producerBinding.unbind();
	}

	@Test
	public void testPublisherAsyncErrorMessageHandler(JCSMPSession jcsmpSession,
													  SpringCloudStreamContext context,
													  SoftAssertions softly,
													  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + context.getDestinationNameDelimiter() + "errors";

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel,
				producerProperties);

		final CountDownLatch errorLatch = new CountDownLatch(1);
		context.createChannel(errorDestination0, PublishSubscribeChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			softly.assertThat(msg).satisfies(isValidProducerErrorMessage(true));
			errorLatch.countDown();
		});

		context.binderBindUnbindLatency();

		Queue queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));

		try {
			EndpointProperties endpointProperties = new EndpointProperties();
			endpointProperties.setMaxMsgSize(1); // force async publish error
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);
			jcsmpSession.addSubscription(queue, JCSMPFactory.onlyInstance().createTopic(destination0),
					JCSMPSession.WAIT_FOR_CONFIRM);

			moduleOutputChannel.send(MessageBuilder.withPayload("foo".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build());
			assertThat(errorLatch.await(10, TimeUnit.SECONDS)).isTrue();
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}

		producerBinding.unbind();
	}

	private boolean hasNestedBooleanHeader(String header, Message<?> message, boolean batchMode) {
		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(message).satisfies(hasNestedHeader(header, Boolean.class,
				batchMode, v -> assertThat(v).isNotNull().isTrue()));
		return softly.wasSuccess();
	}
}
