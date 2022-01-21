package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
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
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * All tests regarding client acknowledgment
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class SolaceBinderClientAckIT<T> {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderClientAckIT.class);

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAccept(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
						   TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg -> {
			AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
			Objects.requireNonNull(ackCallback).noAutoAck();
			AckUtils.accept(ackCallback);
		});

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(context, sempV2Api, binder.getConsumerQueueName(consumerBinding), 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testReject(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
						   SoftAssertions softly, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		AtomicBoolean wasRedelivered = new AtomicBoolean(false);
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRejectWithErrorQueue(Class<T> channelType, JCSMPSession jcsmpSession, SempV2Api sempV2Api,
										 SpringCloudStreamContext context, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRequeue(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
							SoftAssertions softly, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		AtomicBoolean wasRedelivered = new AtomicBoolean(false);
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAsyncAccept(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
								@ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
								TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					logger.info("Received message");
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					executorService.schedule(() -> {
						validateNumEnqueuedMessages(context, sempV2Api, queueName, 1);
						logger.info("Async acknowledging message");
						AckUtils.accept(ackCallback);
						callback.run();
					}, 2, TimeUnit.SECONDS);
				});

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAsyncReject(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
								SoftAssertions softly,
								@ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
								TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

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
							validateNumEnqueuedMessages(context, sempV2Api, queueName, 1);
							AckUtils.reject(ackCallback);
							callback.run();
						}, 2, TimeUnit.SECONDS);
					}
				}, 2);
		softly.assertAll();
		assertThat(wasRedelivered.get()).isTrue();

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAsyncRejectWithErrorQueue(Class<T> channelType, JCSMPSession jcsmpSession, SempV2Api sempV2Api,
											  SpringCloudStreamContext context,
											  @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
											  TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					executorService.schedule(() -> {
						validateNumEnqueuedMessages(context, sempV2Api, queueName, 1);
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAsyncRequeue(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
								 @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
								 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

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
							validateNumEnqueuedMessages(context, sempV2Api, queueName, 1);
							AckUtils.requeue(ackCallback);
							callback.run();
						}, 2, TimeUnit.SECONDS);
					}
				}, 2);
		assertThat(wasRedelivered.get()).isTrue();

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testNoAck(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
						  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message, msg ->
				Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg)).noAutoAck());

		// Give some time just to make sure
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 1);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
		validateNumAckedMessages(context, sempV2Api, queueName, 0);
		validateNumUnackedMessages(context, sempV2Api, queueName, 1);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testNoAckAndThrowException(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
										   TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAcceptAndThrowException(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
											TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRejectAndThrowException(Class<T> channelType, SempV2Api sempV2Api, SpringCloudStreamContext context,
											TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRequeueAndThrowException(Class<T> channelType, SempV2Api sempV2Api,
											 SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testNoAckAndThrowExceptionWithErrorQueue(Class<T> channelType,
														 JCSMPSession jcsmpSession,
														 SempV2Api sempV2Api,
														 SpringCloudStreamContext context,
														 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					callback.run();
					throw new RuntimeException("expected exception");
				}, consumerProperties.getMaxAttempts());

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(errorQueueName));
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);
		validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testAcceptAndThrowExceptionWithErrorQueue(Class<T> channelType,
														  SempV2Api sempV2Api,
														  SpringCloudStreamContext context,
														  TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
				group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);
		validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRejectAndThrowExceptionWithErrorQueue(Class<T> channelType,
														  SempV2Api sempV2Api,
														  SpringCloudStreamContext context,
														  JCSMPSession jcsmpSession,
														  TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, moduleOutputChannel, message,
				(msg, callback) -> {
					AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
					Objects.requireNonNull(ackCallback).noAutoAck();
					AckUtils.reject(ackCallback);
					callback.run();
					throw new RuntimeException("expected exception");
				}, consumerProperties.getMaxAttempts());

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(errorQueueName));
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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);
		validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	public void testRequeueAndThrowExceptionWithErrorQueue(Class<T> channelType,
														   SempV2Api sempV2Api,
														   SpringCloudStreamContext context,
														   TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

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

		validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
		validateNumRedeliveredMessages(context, sempV2Api, queueName, 1);
		validateNumAckedMessages(context, sempV2Api, queueName, 1);
		validateNumUnackedMessages(context, sempV2Api, queueName, 0);
		validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	private void validateNumEnqueuedMessages(SpringCloudStreamContext context,
											 SempV2Api sempV2Api,
											 String queueName,
											 int expectedCount) {
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueueMsgs((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							queueName, null, null, null, null)
					.getData())
					.hasSize(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumAckedMessages(SpringCloudStreamContext context,
										  SempV2Api sempV2Api,
										  String queueName,
										  int expectedCount) {
		try {
			List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
					.getMsgVpnQueueTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							queueName, 2, null, null, null)
					.getData();
			assertThat(txFlows).hasSize(1);
			assertThat(txFlows.get(0).getAckedMsgCount()).isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumUnackedMessages(SpringCloudStreamContext context,
											SempV2Api sempV2Api,
											String queueName,
											int expectedCount) {
		try {
			List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
					.getMsgVpnQueueTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							queueName, 2, null, null, null)
					.getData();
			assertThat(txFlows).hasSize(1);
			assertThat(txFlows.get(0).getUnackedMsgCount()).isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}

	private void validateNumRedeliveredMessages(SpringCloudStreamContext context,
												SempV2Api sempV2Api,
												String queueName,
												int expectedCount) {
		try {
			assertThat(sempV2Api.monitor()
					.getMsgVpnQueue((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
							queueName, null)
					.getData()
					.getRedeliveredMsgCount())
					.isEqualTo(expectedCount);
		} catch (ApiException e) {
			throw new RuntimeException(e);
		}
	}
}
