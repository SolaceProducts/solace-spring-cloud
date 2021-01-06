package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.Queue;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * All tests regarding client acknowledgment
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(InheritedTestsFilteredRunner.ParameterizedRunnerFactory.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderClientAckIT<T> extends SolaceBinderITBase {
	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public Class<T> type;

	public ConsumerInfrastructureUtil<T> consumerInfrastructureUtil;

	@Parameterized.Parameters(name = "{0}")
	public static Collection<?> headerSets() {
		return Arrays.asList(new Object[][]{
				{"Async", DirectChannel.class},
				{"Pollable", PollableSource.class}
		});
	}

	@Before
	public void postParamsSetup() {
		this.consumerInfrastructureUtil = new ConsumerInfrastructureUtil<>(type);
	}

	@Test
	public void testAccept() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, "testAccept", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg -> {
			AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
			Objects.requireNonNull(ackCallback).noAutoAck();
			AckUtils.accept(ackCallback);
		});

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(binder.getConsumerQueueName(consumerBinding), 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testReject() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testReject", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		AtomicBoolean wasRedelivered = new AtomicBoolean(false);
		SoftAssertions softly = new SoftAssertions();
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg -> {
			Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
			softly.assertThat(redelivered).isNotNull();
			if (redelivered) {
				wasRedelivered.set(true);
			} else {
				AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
				Objects.requireNonNull(ackCallback).noAutoAck();
				AckUtils.reject(ackCallback);
			}
		}, 2);
		softly.assertAll();
		assertThat(wasRedelivered.get()).isTrue();

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRejectWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testRejectWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg -> {
			AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
			Objects.requireNonNull(ackCallback).noAutoAck();
			AckUtils.reject(ackCallback);
		});

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(errorQueue);
		errorQueueFlowProperties.setStartState(true);
		FlowReceiver flowReceiver = null;
		try {
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumAckedMessages(queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRequeue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testRequeue", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		AtomicBoolean wasRedelivered = new AtomicBoolean(false);
		SoftAssertions softly = new SoftAssertions();
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg -> {
			Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
			softly.assertThat(redelivered).isNotNull();
			if (redelivered) {
				wasRedelivered.set(true);
			} else {
				AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
				Objects.requireNonNull(ackCallback).noAutoAck();
				AckUtils.requeue(ackCallback);
			}
		}, 2);
		softly.assertAll();
		assertThat(wasRedelivered.get()).isTrue();

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAsyncAccept() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testAsyncAccept", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		try {
			consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
					(msg, callback) -> {
						logger.info("Received message");
						AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
						Objects.requireNonNull(ackCallback).noAutoAck();
						executorService.schedule(() -> {
							validateNumEnqueuedMessages(queueName, 1);
							logger.info("Async acknowledging message");
							AckUtils.accept(ackCallback);
							callback.run();
						}, 2, TimeUnit.SECONDS);
					});

			// Give some time for failed message to ack
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));

			validateNumEnqueuedMessages(queueName, 0);

			producerBinding.unbind();
			consumerBinding.unbind();
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testAsyncReject() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testAsyncReject", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		try {
			SoftAssertions softly = new SoftAssertions();
			AtomicBoolean wasRedelivered = new AtomicBoolean(false);
			consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
					(msg, callback) -> {
						Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
						softly.assertThat(redelivered).isNotNull();
						if (redelivered) {
							wasRedelivered.set(true);
							callback.run();
						} else {
							AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
							Objects.requireNonNull(ackCallback).noAutoAck();
							executorService.schedule(() -> {
								validateNumEnqueuedMessages(queueName, 1);
								AckUtils.reject(ackCallback);
								callback.run();
							}, 2, TimeUnit.SECONDS);
						}
					}, 2);
			softly.assertAll();
			assertThat(wasRedelivered.get()).isTrue();

			// Give some time for failed message to ack
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));

			validateNumEnqueuedMessages(queueName, 0);
			validateNumRedeliveredMessages(queueName, 1);

			producerBinding.unbind();
			consumerBinding.unbind();
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testAsyncRejectWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testAsyncRejectWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		try {
			consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
					(msg, callback) -> {
						AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
						Objects.requireNonNull(ackCallback).noAutoAck();
						executorService.schedule(() -> {
							validateNumEnqueuedMessages(queueName, 1);
							AckUtils.reject(ackCallback);
							callback.run();
						}, 2, TimeUnit.SECONDS);
					});

			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setEndpoint(errorQueue);
			errorQueueFlowProperties.setStartState(true);
			FlowReceiver flowReceiver = null;
			try {
				flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
				assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
			} finally {
				if (flowReceiver != null) {
					flowReceiver.close();
				}
			}

			// Give some time for the message to actually ack off the original queue
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));

			validateNumEnqueuedMessages(queueName, 0);

			producerBinding.unbind();
			consumerBinding.unbind();
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testAsyncRequeue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testAsyncRequeue", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
		try {
			AtomicBoolean wasRedelivered = new AtomicBoolean(false);
			consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
					(msg, callback) -> {
						Boolean redelivered = msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class);
						assertThat(redelivered).isNotNull();
						if (redelivered) {
							wasRedelivered.set(true);
							callback.run();
						} else {
							AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
							Objects.requireNonNull(ackCallback).noAutoAck();
							executorService.schedule(() -> {
								validateNumEnqueuedMessages(queueName, 1);
								AckUtils.requeue(ackCallback);
								callback.run();
							}, 2, TimeUnit.SECONDS);
						}
					}, 2);
			assertThat(wasRedelivered.get()).isTrue();

			// Give some time for failed message to ack
			Thread.sleep(TimeUnit.SECONDS.toMillis(3));

			validateNumEnqueuedMessages(queueName, 0);
			validateNumRedeliveredMessages(queueName, 1);

			producerBinding.unbind();
			consumerBinding.unbind();
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testNoAck() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testNoAck", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg ->
				Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg)).noAutoAck());

		// Give some time just to make sure
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 1);
		validateNumRedeliveredMessages(queueName, 0);
		validateNumAckedMessages(queueName, 0);
		validateNumUnackedMessages(queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testNoAckAndThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testNoAckAndThrowException", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						callback.run();
					} else {
						logger.info("Received message");
						Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg)).noAutoAck();
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts() + 1);

		// Give some time just to make sure
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAcceptAndThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testAcceptAndThrowException", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		final CountDownLatch redeliveredLatch = new CountDownLatch(1);
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(acknowledgmentCallback).noAutoAck();
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						redeliveredLatch.countDown();
					} else {
						logger.info("Receiving message");
						AckUtils.accept(acknowledgmentCallback);
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts());
		assertThat(redeliveredLatch.await(3, TimeUnit.SECONDS)).isFalse();

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 0);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRejectAndThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				"testRejectAndThrowException", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						callback.run();
					} else {
						logger.info("Receiving message");
						AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
						Objects.requireNonNull(ackCallback).noAutoAck();
						AckUtils.reject(ackCallback);
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts() + 1);

		// Give some time for the message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRequeueAndThrowException() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
		"testRequeueAndThrowException", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						callback.run();
					} else {
						logger.info("Receiving message");
						AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
						Objects.requireNonNull(ackCallback).noAutoAck();
						AckUtils.requeue(ackCallback);
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts() + 1);

		// Give some time for the message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testNoAckAndThrowExceptionWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testNoAckAndThrowExceptionWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					callback.run();
					throw new RuntimeException("expected exception");
				}, consumerProperties.getMaxAttempts());

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(errorQueue);
		errorQueueFlowProperties.setStartState(true);
		FlowReceiver flowReceiver = null;
		try {
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 0);
		validateNumUnackedMessages(queueName, 0);
		validateNumEnqueuedMessages(errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAcceptAndThrowExceptionWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testAcceptAndThrowExceptionWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch redeliveredLatch = new CountDownLatch(1);
		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(acknowledgmentCallback).noAutoAck();
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						redeliveredLatch.countDown();
					} else {
						logger.info("Receiving message");
						AckUtils.accept(acknowledgmentCallback);
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts());

		assertThat(redeliveredLatch.await(3, TimeUnit.SECONDS)).isFalse();

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 0);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);
		validateNumEnqueuedMessages(errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRejectAndThrowExceptionWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testRejectAndThrowExceptionWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					AckUtils.reject(ackCallback);
					callback.run();
					throw new RuntimeException("expected exception");
				}, consumerProperties.getMaxAttempts());

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(errorQueue);
		errorQueueFlowProperties.setStartState(true);
		FlowReceiver flowReceiver = null;
		try {
			flowReceiver = jcsmpSession.createFlow(null, errorQueueFlowProperties);
			assertThat(flowReceiver.receive((int) TimeUnit.SECONDS.toMillis(10))).isNotNull();
		} finally {
			if (flowReceiver != null) {
				flowReceiver.close();
			}
		}

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 0);
		validateNumUnackedMessages(queueName, 0);
		validateNumEnqueuedMessages(errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testRequeueAndThrowExceptionWithErrorQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testRequeueAndThrowExceptionWithErrorQueue";

		String queueName = destination0 + getDestinationNameDelimiter() + group0;
		String errorQueueName = queueName + getDestinationNameDelimiter() + "error";

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					if (Objects.requireNonNull(msg.getHeaders().get(SolaceHeaders.REDELIVERED, Boolean.class))) {
						logger.info("Received redelivered message");
						callback.run();
					} else {
						logger.info("Receiving message");
						AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
						Objects.requireNonNull(ackCallback).noAutoAck();
						AckUtils.requeue(ackCallback);
						callback.run();
						throw new RuntimeException("expected exception");
					}
				}, consumerProperties.getMaxAttempts() + 1);

		// Give some time for the message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(queueName, 0);
		validateNumRedeliveredMessages(queueName, 1);
		validateNumAckedMessages(queueName, 1);
		validateNumUnackedMessages(queueName, 0);
		validateNumEnqueuedMessages(errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	private void validateNumEnqueuedMessages(String queueName, int expectedCount) {
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs(msgVpnName, queueName, null, null, null, null)
					.getData())
					.hasSize(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumAckedMessages(String queueName, int expectedCount) {
		try {
			List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
					.getMsgVpnQueueTxFlows(msgVpnName, queueName, 2, null, null, null)
					.getData();
			assertThat(txFlows).hasSize(1);
			assertThat(txFlows.get(0).getAckedMsgCount()).isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumUnackedMessages(String queueName, int expectedCount) {
		try {
			List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
					.getMsgVpnQueueTxFlows(msgVpnName, queueName, 2, null, null, null)
					.getData();
			assertThat(txFlows).hasSize(1);
			assertThat(txFlows.get(0).getUnackedMsgCount()).isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumRedeliveredMessages(String queueName, int expectedCount) {
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(msgVpnName, queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}
}
