package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsg;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPInterruptedException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Requestor;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessageConsumer;
import com.solacesystems.jcsmp.XMLMessageListener;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static com.solace.spring.cloud.stream.binder.test.util.ValuePoller.poll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.is;
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

	// NOT YET SUPPORTED ---------------------------------
	@Override
	@Test
	@Execution(ExecutionMode.SAME_THREAD)
	@Disabled("Partitioning is not supported")
	public void testPartitionedModuleSpEL(TestInfo testInfo) throws Exception {
		super.testPartitionedModuleSpEL(testInfo);
	}
	// ---------------------------------------------------

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testSendAndReceiveBad(SoftAssertions softly, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(msg -> {
			long expectedDeliveryAttempt = consumerProperties.getMaxAttempts() - latch.getCount() + 1;
			AtomicInteger deliveryAttempt = StaticMessageHeaderAccessor.getDeliveryAttempt(msg);
			softly.assertThat(deliveryAttempt).isNotNull();
			softly.assertThat(deliveryAttempt.get()).isEqualTo(expectedDeliveryAttempt);
			latch.countDown();
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testProducerErrorChannel(JCSMPSession jcsmpSession, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination0EC = String.format("%s%serrors", destination0, getDestinationNameDelimiter());

		ExtendedProducerProperties<SolaceProducerProperties> producerProps = createProducerProperties(testInfo);
		producerProps.setErrorChannelEnabled(true);
		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProps);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		final CountDownLatch latch = new CountDownLatch(2);

		final AtomicReference<Message<?>> errorMessage = new AtomicReference<>();
		binder.getApplicationContext()
				.getBean(destination0EC, SubscribableChannel.class)
				.subscribe(message1 -> {
					errorMessage.set(message1);
					latch.countDown();
				});

		binder.getApplicationContext()
				.getBean(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME, SubscribableChannel.class)
				.subscribe(message12 -> latch.countDown());

		jcsmpSession.closeSession();

		try {
			moduleOutputChannel.send(message);
			fail("Expected the producer to fail to send the message...");
		} catch (Exception e) {
			logger.info("Successfully threw an exception during message publishing!");
		}

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(errorMessage.get()).isInstanceOf(ErrorMessage.class);
		assertThat(errorMessage.get().getPayload()).isInstanceOf(MessagingException.class);
		assertThat((MessagingException) errorMessage.get().getPayload()).hasCauseInstanceOf(ClosedFacilityException.class);
		producerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testConsumerRequeue(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final AtomicInteger numRetriesRemaining = new AtomicInteger(consumerProperties.getMaxAttempts());
		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			if (numRetriesRemaining.getAndDecrement() > 0) {
				throw new RuntimeException("Throwing expected exception!");
			} else {
				logger.info("Received message");
				latch.countDown();
			}
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testConsumerErrorQueueRepublish(JCSMPSession jcsmpSession, SempV2Api sempV2Api, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		moduleInputChannel.subscribe(message1 -> {
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
				binder.getConsumerErrorQueueName(consumerBinding)));
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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testAnonConsumerDiscard(JCSMPSession jcsmpSession, SempV2Api sempV2Api, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String queueName = binder.getConsumerQueueName(consumerBinding);

		Long flowId = sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, 2, null, null, null)
				.getData()
				.get(0)
				.getFlowId();

		final CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			assertThat(latch.getCount()).isNotEqualTo(0);
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(2, TimeUnit.MINUTES)).isTrue();

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, 2, null, null, null)
				.getData();
		assertThat(txFlows).hasSize(1);
		assertThat(txFlows.get(0).getFlowId()).isEqualTo(flowId); // i.e. flow was not rebound

		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getSpooledMsgCount())
				.isEqualTo(1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, 2, null, null, null)
				.getData())
				.hasSize(0); // i.e. message was discarded

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testAnonConsumerErrorQueueRepublish(JCSMPSession jcsmpSession, SempV2Api sempV2Api, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		moduleInputChannel.subscribe(message1 -> {
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
						queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testPolledConsumer(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, "testPolledConsumer", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(message1 -> logger.info(String.format("Received message %s", message1)));
		}
		assertThat(gotMessage).isTrue();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testPolledConsumerRequeue(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, "testPolledConsumerRequeue", moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		retryAssert(() -> assertThat(moduleInputChannel.poll(message1 -> {
			throw new RuntimeException("Throwing expected exception!");
		})).isTrue());

		retryAssert(() -> assertThat(moduleInputChannel.poll(message1 ->
				logger.info(String.format("Received message %s", message1)))).isTrue());

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testPolledConsumerErrorQueueRepublish(JCSMPSession jcsmpSession, SempV2Api sempV2Api, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(message1 -> {
				throw new RuntimeException("Throwing expected exception!");
			});
		}
		assertThat(gotMessage).isTrue();

		final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
		errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
				binder.getConsumerErrorQueueName(consumerBinding)));
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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, binder.getConsumerQueueName(consumerBinding), 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testPolledAnonConsumerDiscard(JCSMPSession jcsmpSession, SempV2Api sempV2Api, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String queueName = binder.getConsumerQueueName(consumerBinding);

		Long flowId = sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, 2, null, null, null)
				.getData()
				.get(0)
				.getFlowId();

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(message1 -> {
				throw new RuntimeException("Throwing expected exception!");
			});
		}
		assertThat(gotMessage).isTrue();

		// Give some time for failed message to ack
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, 2, null, null, null)
				.getData();
		assertThat(txFlows).hasSize(1);
		assertThat(txFlows.get(0).getFlowId()).isEqualTo(flowId); // i.e. flow was not rebound

		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getSpooledMsgCount())
				.isEqualTo(1);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, 2, null, null, null)
				.getData())
				.hasSize(0); // i.e. message was discarded

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	@Execution(ExecutionMode.CONCURRENT)
	public void testPolledAnonConsumerErrorQueueRepublish(JCSMPSession jcsmpSession, SempV2Api sempV2Api,
														  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		boolean gotMessage = false;
		for (int i = 0; !gotMessage && i < 100; i++) {
			gotMessage = moduleInputChannel.poll(message1 -> {
				throw new RuntimeException("Throwing expected exception!");
			});
		}
		assertThat(gotMessage).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
						queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

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
		Map<String,String[]> groupsAdditionalSubs = new HashMap<>();
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

	@RetryingTest(maxAttempts = 10, onExceptions = AssertionError.class) // flaky when ran in parallel
	@Execution(ExecutionMode.SAME_THREAD)
	public void testConsumerReconnect(JCSMPSession jcsmpSession, SempV2Api sempV2Api, SoftAssertions softly,
									  @ExecSvc(poolSize = 1) ExecutorService executor,
									  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		binderBindUnbindLatency();

		String queue0 = binder.getConsumerQueueName(consumerBinding);

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		moduleInputChannel.subscribe(message1 -> {
			numMsgsConsumed.incrementAndGet();
			String payload = new String((byte[]) message1.getPayload());
			logger.trace(String.format("Received message %s", payload));
			uniquePayloadsReceived.add(payload);
		});

		Future<Integer> future = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!Thread.currentThread().isInterrupted()) {
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
		executor.shutdown();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Disabling egress to queue %s", queue0));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().egressEnabled(false), null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info(String.format("Enabling egress to queue %s", queue0));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().egressEnabled(true), null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Stopping producer");
		executor.shutdownNow();
		assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
		int numMsgsSent = future.get(5, TimeUnit.SECONDS);

		assertThat(poll(() -> sempV2Api.monitor().getMsgVpnQueueMsgs(vpnName, queue0, Integer.MAX_VALUE,
				null, null, null).getData().size())
				.until(is(0))
				.execute()
				.get())
				.as("Expected queue %s to be empty after rebind", queue0)
				.isEqualTo(0);

		MonitorMsgVpnQueue queueState = sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData();

		softly.assertThat(queueState.getDisabledBindFailureCount()).isGreaterThan(0);
		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		// -2 margin of error. Redelivered messages might be untracked if it's consumer was shutdown before it could
		// be added to the consumed msg count.
		softly.assertThat(numMsgsConsumed.get() - queueState.getRedeliveredMsgCount())
				.isBetween((long)numMsgsSent-2, (long)numMsgsSent);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@RetryingTest(maxAttempts = 10, onExceptions = AssertionError.class) // flaky when ran in parallel
	@Execution(ExecutionMode.SAME_THREAD)
	public void testConsumerRebind(JCSMPSession jcsmpSession, SempV2Api sempV2Api, SoftAssertions softly,
								   @ExecSvc(poolSize = 1) ExecutorService executor,
								   TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		binderBindUnbindLatency();

		String queue0 = binder.getConsumerQueueName(consumerBinding);

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		moduleInputChannel.subscribe(message1 -> {
			numMsgsConsumed.incrementAndGet();
			String payload = new String((byte[]) message1.getPayload());
			logger.trace(String.format("Received message %s", payload));
			uniquePayloadsReceived.add(payload);
		});

		Future<Integer> future = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!Thread.currentThread().isInterrupted()) {
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
		executor.shutdown();

		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Unbinding consumer");
		consumerBinding.unbind();

		logger.info("Stopping producer");
		executor.shutdownNow();
		assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		logger.info("Rebinding consumer");
		consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, createConsumerProperties());
		Thread.sleep(TimeUnit.SECONDS.toMillis(5));

		int numMsgsSent = future.get(5, TimeUnit.SECONDS);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		assertThat(poll(() -> sempV2Api.monitor().getMsgVpnQueueMsgs(vpnName, queue0, Integer.MAX_VALUE,
								null, null, null).getData().size())
						.until(is(0))
						.execute()
						.get())
				.as("Expected queue %s to be empty after rebind", queue0)
				.isEqualTo(0);

		long redeliveredMsgs = sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queue0, null)
				.getData()
				.getRedeliveredMsgCount();

		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		// -2 margin of error. Redelivered messages might be untracked if it's consumer was shutdown before it could
		// be added to the consumed msg count.
		softly.assertThat(numMsgsConsumed.get() - redeliveredMsgs).isBetween((long)numMsgsSent-2, (long)numMsgsSent);

		producerBinding.unbind();
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
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(requestDestination, "boo",
				moduleInputChannel, createConsumerProperties());

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
					.setHeader(BinderHeaders.TARGET_DESTINATION, reqReplyTo.getName())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();
			moduleOutputChannel.send(springMessage);
		});

		//Prepare the Requestor (need a producer and a consumer)
		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
			@Override
			public void responseReceivedEx(Object o) {
				logger.info("Request sent successfully");
			}

			@Override
			public void handleErrorEx(Object o, JCSMPException e, long l) {}
		});
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

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseResume(Class<T> channelType,
									JCSMPSession jcsmpSession,
									SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);

		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		consumerBinding.pause();
		assertEquals(0, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		consumerBinding.resume();
		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseBeforeConsumerStart(Class<T> channelType,
												 JCSMPSession jcsmpSession,
												 SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setAutoStartup(false);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		String queueName = binder.getConsumerQueueName(consumerBinding);

		assertThat(consumerBinding.isRunning()).isFalse();
		assertThat(consumerBinding.isPaused()).isFalse();

		consumerBinding.pause();
		assertThat(consumerBinding.isRunning()).isFalse();
		assertThat(consumerBinding.isPaused()).isTrue();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);

		consumerBinding.start();
		assertThat(consumerBinding.isRunning()).isTrue();
		assertThat(consumerBinding.isPaused()).isTrue();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1))
				.hasSize(1)
				.allSatisfy(flow -> assertThat(flow.getWindowSize()).isEqualTo(0));

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseStateIsMaintainedWhenConsumerIsRestarted(Class<T> channelType,
																	  JCSMPSession jcsmpSession,
																	  SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);
		consumerBinding.pause();
		assertEquals(0, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.stop();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		//Newly created flow is started in the pause state
		assertEquals(0, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseOnStoppedConsumer(Class<T> channelType,
											   JCSMPSession jcsmpSession,
											   SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.stop();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.pause();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		assertEquals(0, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testResumeOnStoppedConsumer(Class<T> channelType,
												JCSMPSession jcsmpSession,
												SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.pause();
		consumerBinding.stop();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.resume();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.start();
		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testPauseResumeOnStoppedConsumer(Class<T> channelType,
													 JCSMPSession jcsmpSession,
													 SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		int defaultWindowSize = (int) jcsmpSession.getProperty(JCSMPProperties.SUB_ACK_WINDOW_SIZE);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.stop();
		assertThat(getTxFlows(sempV2Api, vpnName, queueName, 1)).hasSize(0);
		consumerBinding.pause(); //Has no effect
		consumerBinding.resume(); //Has no effect
		consumerBinding.start();
		assertEquals(defaultWindowSize, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());

		consumerBinding.unbind();
	}

	@ParameterizedTest
	@ValueSource(classes = {DirectChannel.class, PollableSource.class})
	@Execution(ExecutionMode.CONCURRENT)
	public <T> void testFailResumeOnClosedConsumer(Class<T> channelType,
												   JCSMPSession jcsmpSession,
												   SempV2Api sempV2Api) throws Exception {
		SolaceTestBinder binder = getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = createConsumerInfrastructureUtil(channelType);
		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, createConsumerProperties());

		String queueName = binder.getConsumerQueueName(consumerBinding);

		consumerBinding.pause();
		assertEquals(0, getTxFlows(sempV2Api, vpnName, queueName, 1).get(0).getWindowSize());
		getJcsmpSession().closeSession();
		RuntimeException exception = assertThrows(RuntimeException.class, consumerBinding::resume);
		assertThat(exception).hasRootCauseInstanceOf(ClosedFacilityException.class);
		assertThat(consumerBinding.isPaused())
				.as("Failed resume should leave the binding in a paused state")
				.isTrue();

		consumerBinding.unbind();
	}

	private List<MonitorMsgVpnQueueTxFlow> getTxFlows(SempV2Api sempV2Api, String vpnName, String queueName, Integer count) throws ApiException {
		return sempV2Api.monitor()
				.getMsgVpnQueueTxFlows(vpnName, queueName, count, null, null, null)
				.getData();
	}
}
