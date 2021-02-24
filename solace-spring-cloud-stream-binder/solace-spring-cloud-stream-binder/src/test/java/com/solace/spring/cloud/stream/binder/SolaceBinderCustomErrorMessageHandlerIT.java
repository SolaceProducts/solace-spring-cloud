package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.RequeueCurrentMessageException;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHandlingException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/**
 * All tests regarding custom channel-specific error message handlers
 * (i.e. overriding {@link SolaceErrorMessageHandler}).
 */
@RunWith(InheritedTestsFilteredRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderCustomErrorMessageHandlerIT extends SolaceBinderITBase {
	@Test
	public void testConsumerOverrideErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		final CountDownLatch errorLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			applyErrorMessageAssertions(msg, softly, true);
			errorLatch.countDown();
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		final CountDownLatch consumeLatch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			consumeLatch.countDown();
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);
		assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
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

	@Test
	public void testConsumerOverrideErrorMessageHandlerThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RuntimeException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);

		FlowReceiver flowReceiver = null;
		try {
			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
					binder.getConsumerErrorQueueName(consumerBinding)));
			errorQueueFlowProperties.setStartState(true);
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

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

	@Test
	public void testConsumerOverrideErrorMessageHandlerThrowRequeueException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RequeueCurrentMessageException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
			if (redelivered != null && redelivered) {
				latch.countDown();
			} else {
				throw new RuntimeException("bad");
			}
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(1);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerOverrideErrorMessageHandlerThrowExceptionAndStale() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		CountDownLatch continueLatch = new CountDownLatch(1);
		CountDownLatch errorStartLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			errorStartLatch.countDown();
			try {
				softly.assertThat(continueLatch.await(1, TimeUnit.MINUTES)).isTrue();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			throw new RuntimeException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(1);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		consumerProperties.getExtension().setFlowPreRebindWaitTimeout(0);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message1 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader("skip", true)
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Message<?> message2 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		CompletableFuture<AcknowledgmentCallback> staleTriggeringAckFuture = new CompletableFuture<>();

		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
			if (redelivered == null || !redelivered) {
				if (msg.getHeaders().containsKey("skip")) {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					softly.assertThat(ackCallback).isNotNull();
					ackCallback.noAutoAck();
					staleTriggeringAckFuture.complete(ackCallback);
				} else {
					throw new RuntimeException("bad");
				}
			}
		});

		moduleOutputChannel.send(message1);
		moduleOutputChannel.send(message2);

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
					.isEqualTo(2);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, errorQueueName, 1, null, null, null)
					.getData())
					.hasSize(0);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerOverrideRetryableErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		final CountDownLatch errorLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			applyErrorMessageAssertions(msg, softly, true);
			errorLatch.countDown();
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		final CountDownLatch consumeLatch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			consumeLatch.countDown();
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);
		assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
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

	@Test
	public void testConsumerOverrideRetryableErrorMessageHandlerThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RuntimeException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);

		FlowReceiver flowReceiver = null;
		try {
			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
					binder.getConsumerErrorQueueName(consumerBinding)));
			errorQueueFlowProperties.setStartState(true);
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

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

	@Test
	public void testConsumerOverrideRetryableErrorMessageHandlerThrowRequeueException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RequeueCurrentMessageException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setMaxAttempts(3);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(msg -> {
			logger.info(String.format("Received message %s", msg));
			Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
			if (redelivered != null && redelivered) {
				latch.countDown();
			} else {
				throw new RuntimeException("bad");
			}
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();

		retryAssert(() -> {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
					.getData())
					.hasSize(0);
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(1);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerOverridePollableErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		final CountDownLatch errorLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			applyErrorMessageAssertions(msg, softly, true);
			errorLatch.countDown();
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(msg -> {
				logger.info(String.format("Received message %s", msg));
				throw new RuntimeException("bad");
			});
		}
		assertThat(gotMessage).isTrue();
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

	@Test
	public void testConsumerOverridePollableErrorMessageHandlerThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RuntimeException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		retryAssert(() -> assertThrows(MessageHandlingException.class, () -> moduleInputChannel.poll(msg -> {
			logger.info(String.format("Received message %s", msg));
			throw new RuntimeException("bad");
		})));

		FlowReceiver flowReceiver = null;
		try {
			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
					binder.getConsumerErrorQueueName(consumerBinding)));
			errorQueueFlowProperties.setStartState(true);
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

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

	@Test
	public void testConsumerOverridePollableErrorMessageHandlerThrowRequeueException() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + group0 +
				getDestinationNameDelimiter() + "errors";
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		// Need to create channel before so that the override actually works
		createChannel(errorDestination0, DirectChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			throw new RequeueCurrentMessageException("test");
		});

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(msg -> {
				logger.info(String.format("Received message %s", msg));
				throw new RuntimeException("bad");
			});
		}

		SoftAssertions softly = new SoftAssertions();
		boolean gotSuccessMessage = false;
		for (int i = 0; !gotSuccessMessage && i < 100; i++) {
			gotSuccessMessage = moduleInputChannel.poll(msg -> {
				logger.info(String.format("Received message %s", msg));
				softly.assertThat(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class)).isTrue();
			});
		}
		assertThat(gotSuccessMessage).isTrue();
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
					.isEqualTo(1);
		});

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testPublisherErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + "errors";

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel,
				producerProperties);

		final CountDownLatch errorLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, PublishSubscribeChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			applyErrorMessageAssertions(msg, softly, false);
			errorLatch.countDown();
		});

		binderBindUnbindLatency();

		assertThrows(MessagingException.class, () -> moduleOutputChannel.send(
				MessageBuilder.withPayload("foo".getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.setHeader(BinderHeaders.TARGET_DESTINATION, new Object()) // force a publish error
						.build()));
		assertThat(errorLatch.await(10, TimeUnit.SECONDS)).isTrue();
		softly.assertAll();

		producerBinding.unbind();
	}

	@Test
	public void testPublisherAsyncErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String errorDestination0 = destination0 + getDestinationNameDelimiter() + "errors";

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		producerProperties.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel,
				producerProperties);

		final CountDownLatch errorLatch = new CountDownLatch(1);
		SoftAssertions softly = new SoftAssertions();
		createChannel(errorDestination0, PublishSubscribeChannel.class, msg -> {
			logger.info("Got error message: " + msg);
			applyErrorMessageAssertions(msg, softly, true);
			errorLatch.countDown();
		});

		binderBindUnbindLatency();

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
			softly.assertAll();
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}

		producerBinding.unbind();
	}

	private void applyErrorMessageAssertions(Message<?> errorMessage, SoftAssertions softAssertions,
											 boolean expectRawMessageHeader) {
		softAssertions.assertThat(errorMessage).isInstanceOf(ErrorMessage.class);
		softAssertions.assertThat(((ErrorMessage) errorMessage).getOriginalMessage()).isNotNull();
		softAssertions.assertThat(((ErrorMessage) errorMessage).getPayload()).isNotNull();
		if (expectRawMessageHeader) {
			softAssertions.assertThat((Object) StaticMessageHeaderAccessor.getSourceData(errorMessage))
					.isInstanceOf(XMLMessage.class);
		} else {
			softAssertions.assertThat(errorMessage.getHeaders())
					.doesNotContainKey(IntegrationMessageHeaderAccessor.SOURCE_DATA);
		}
	}
}
