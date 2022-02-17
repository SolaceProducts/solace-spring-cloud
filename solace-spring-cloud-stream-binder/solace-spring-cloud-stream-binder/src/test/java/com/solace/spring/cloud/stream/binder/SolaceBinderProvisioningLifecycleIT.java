package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.test.util.ThrowingFunction;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueMsg;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlowResponse;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageListener;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * All tests which modify the default provisioning lifecycle.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderProvisioningLifecycleIT {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderProvisioningLifecycleIT.class);

	@Test
	public void testConsumerProvisionDurableQueue(JCSMPSession jcsmpSession, SpringCloudStreamContext context,
												  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueNames(destination0, group0, consumerProperties.getExtension(), false)
				.getConsumerGroupQueueName());
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning queue %s with Permission %s", queue.getName(), endpointProperties.getPermission()));
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

			Message<?> message = MessageBuilder.withPayload("foo".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			context.binderBindUnbindLatency();

			final CountDownLatch latch = new CountDownLatch(1);
			moduleInputChannel.subscribe(message1 -> {
				logger.info(String.format("Received message %s", message1));
				latch.countDown();
			});

			moduleOutputChannel.send(message);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testProducerProvisionDurableQueue(JCSMPSession jcsmpSession, SpringCloudStreamContext context,
												  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		assertThat(producerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		producerProperties.getExtension().setProvisionDurableQueue(false);
		producerProperties.setRequiredGroups(group0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setProvisionDurableQueue(false);
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueName(destination0, group0, producerProperties.getExtension()));
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning queue %s with Permission %s", queue.getName(), endpointProperties.getPermission()));
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);
			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

			Message<?> message = MessageBuilder.withPayload("foo".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			context.binderBindUnbindLatency();

			final CountDownLatch latch = new CountDownLatch(1);
			moduleInputChannel.subscribe(message1 -> {
				logger.info(String.format("Received message %s", message1));
				latch.countDown();
			});

			moduleOutputChannel.send(message);
			assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testPolledConsumerProvisionDurableQueue(JCSMPSession jcsmpSession, SpringCloudStreamContext context,
														TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
				.getQueueNames(destination0, group0, consumerProperties.getExtension(), false)
				.getConsumerGroupQueueName());
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);

		Binding<MessageChannel> producerBinding = null;
		Binding<PollableSource<MessageHandler>> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning queue %s with Permission %s", queue.getName(), endpointProperties.getPermission()));
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
			consumerBinding = binder.bindPollableConsumer(destination0, group0, moduleInputChannel, consumerProperties);

			Message<?> message = MessageBuilder.withPayload("foo".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			context.binderBindUnbindLatency();

			logger.info(String.format("Sending message to destination %s: %s", destination0, message));
			moduleOutputChannel.send(message);

			boolean gotMessage = false;
			for (int i = 0; !gotMessage && i < 100; i++) {
				gotMessage = moduleInputChannel.poll(message1 -> logger.info(String.format("Received message %s", message1)));
			}
			assertThat(gotMessage).isTrue();
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testAnonConsumerProvisionDurableQueue(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false); // Expect this parameter to do nothing

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testFailConsumerProvisioningOnDisablingProvisionDurableQueue(SpringCloudStreamContext context)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to missing queue");
		} catch (ProvisioningException e) {
			int expectedSubcodeEx = JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME;
			assertThat(e).hasCauseInstanceOf(JCSMPErrorResponseException.class);
			assertThat(((JCSMPErrorResponseException) e.getCause()).getSubcodeEx()).isEqualTo(expectedSubcodeEx);
			logger.info(String.format("Successfully threw a %s exception with cause %s, subcode: %s",
					ProvisioningException.class.getSimpleName(), JCSMPErrorResponseException.class.getSimpleName(),
					JCSMPErrorResponseSubcodeEx.getSubcodeAsString(expectedSubcodeEx)));
		}
	}

	@Test
	public void testFailProducerProvisioningOnDisablingProvisionDurableQueue(SpringCloudStreamContext context,
																			 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		assertThat(producerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		producerProperties.getExtension().setProvisionDurableQueue(false);
		producerProperties.setRequiredGroups(group0);

		try {
			Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);
			producerBinding.unbind();
			fail("Expected producer provisioning to fail due to missing queue");
		} catch (ProvisioningException e) {
			int expectedSubcodeEx = JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME;
			assertThat(e).hasCauseInstanceOf(JCSMPErrorResponseException.class);
			assertThat(((JCSMPErrorResponseException) e.getCause()).getSubcodeEx()).isEqualTo(expectedSubcodeEx);
			logger.info(String.format("Successfully threw a %s exception with cause %s, subcode: %s",
					ProvisioningException.class.getSimpleName(), JCSMPErrorResponseException.class.getSimpleName(),
					JCSMPErrorResponseSubcodeEx.getSubcodeAsString(expectedSubcodeEx)));
		}
	}

	@Test
	public void testFailPolledConsumerProvisioningOnDisablingProvisionDurableQueue(SpringCloudStreamContext context)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		try {
			Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected polled consumer provisioning to fail due to missing queue");
		} catch (ProvisioningException e) {
			int expectedSubcodeEx = JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME;
			assertThat(e).hasCauseInstanceOf(JCSMPErrorResponseException.class);
			assertThat(((JCSMPErrorResponseException) e.getCause()).getSubcodeEx()).isEqualTo(expectedSubcodeEx);
			logger.info(String.format("Successfully threw a %s exception with cause %s, subcode: %s",
					ProvisioningException.class.getSimpleName(), JCSMPErrorResponseException.class.getSimpleName(),
					JCSMPErrorResponseSubcodeEx.getSubcodeAsString(expectedSubcodeEx)));
		}
	}

	@Test
	public void testConsumerProvisionErrorQueue(JCSMPSession jcsmpSession, SpringCloudStreamContext context)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionErrorQueue()).isTrue();
		consumerProperties.getExtension().setProvisionErrorQueue(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);

		String errorQueueName = SolaceProvisioningUtil
				.getQueueNames(destination0, group0, consumerProperties.getExtension(), false)
				.getErrorQueueName();
		Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(errorQueueName);

		Binding<MessageChannel> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning error queue %s", errorQueue.getName()));
			jcsmpSession.provision(errorQueue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);

			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);
			context.binderBindUnbindLatency();
		} finally {
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(errorQueue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testFailConsumerProvisioningOnDisablingProvisionErrorQueue(SpringCloudStreamContext context)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionErrorQueue()).isTrue();
		consumerProperties.getExtension().setProvisionErrorQueue(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to missing error queue");
		} catch (ProvisioningException e) {
			int expectedSubcodeEx = JCSMPErrorResponseSubcodeEx.UNKNOWN_QUEUE_NAME;
			assertThat(e).hasCauseInstanceOf(JCSMPErrorResponseException.class);
			assertThat(((JCSMPErrorResponseException) e.getCause()).getSubcodeEx()).isEqualTo(expectedSubcodeEx);
			logger.info(String.format("Successfully threw a %s exception with cause %s, subcode: %s",
					ProvisioningException.class.getSimpleName(), JCSMPErrorResponseException.class.getSimpleName(),
					JCSMPErrorResponseSubcodeEx.getSubcodeAsString(expectedSubcodeEx)));
		}
	}

	@Test
	public void testConsumerProvisionSubscriptionsToDurableQueue(JCSMPSession jcsmpSession,
																 SpringCloudStreamContext context,
																 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

		Queue queue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerQueueName(consumerBinding));
		Topic topic = JCSMPFactory.onlyInstance().createTopic(destination0);
		logger.info(String.format("Subscribing queue %s to topic %s", queue.getName(), topic.getName()));
		jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);

		logger.info(String.format("Sending message to destination %s", destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testProducerProvisionSubscriptionsToDurableQueue(JCSMPSession jcsmpSession,
																 SpringCloudStreamContext context,
																 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		assertThat(producerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		producerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);
		producerProperties.setRequiredGroups(group0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setProvisionDurableQueue(false);
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

		Queue queue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerQueueName(consumerBinding));
		Topic topic = JCSMPFactory.onlyInstance().createTopic(destination0);
		logger.info(String.format("Subscribing queue %s to topic %s", queue.getName(), topic.getName()));
		jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);

		logger.info(String.format("Sending message to destination %s", destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testPolledConsumerProvisionSubscriptionsToDurableQueue(JCSMPSession jcsmpSession,
																	   SpringCloudStreamContext context,
																	   TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);

		for (int i = 0; i < 10; i++) {
			TimeUnit.SECONDS.sleep(1);
			moduleInputChannel.poll(message1 -> fail(String.format("Didn't expect to receive message %s", message1)));
		}

		Queue queue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerQueueName(consumerBinding));
		Topic topic = JCSMPFactory.onlyInstance().createTopic(destination0);
		logger.info(String.format("Subscribing queue %s to topic %s", queue.getName(), topic.getName()));
		jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);

		logger.info(String.format("Sending message to destination %s", destination0));
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
	public void testAnonConsumerProvisionSubscriptionsToDurableQueue(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false); // Expect this parameter to do nothing

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	@Execution(ExecutionMode.SAME_THREAD)
	public void testConsumerConcurrency(boolean batchMode,
										SempV2Api sempV2Api,
										SpringCloudStreamContext context,
										SoftAssertions softly,
										TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		int consumerConcurrency = 3;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.setConcurrency(consumerConcurrency);
		if (!consumerProperties.isBatchMode()) {
			consumerProperties.getExtension().setBatchMaxSize(1);
		}
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		int numMsgsPerFlow = 10 * consumerProperties.getExtension().getBatchMaxSize();
		Set<Message<?>> messages = new HashSet<>();
		Map<String, Instant> foundPayloads = new HashMap<>();
		for (int i = 0; i < numMsgsPerFlow * consumerConcurrency; i++) {
			String payload = "foo-" + i;
			foundPayloads.put(payload, null);
			messages.add(MessageBuilder.withPayload(payload.getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build());
		}

		context.binderBindUnbindLatency();

		String queue0 = binder.getConsumerQueueName(consumerBinding);

		final long stallIntervalInMillis = 100;
		final CyclicBarrier barrier = new CyclicBarrier(consumerConcurrency);
		final CountDownLatch latch = new CountDownLatch(numMsgsPerFlow * consumerConcurrency);
		moduleInputChannel.subscribe(message1 -> {
			@SuppressWarnings("unchecked")
			List<byte[]> payloads = consumerProperties.isBatchMode() ? (List<byte[]>) message1.getPayload() :
					Collections.singletonList((byte[]) message1.getPayload());
			for (byte[] payloadBytes : payloads) {
				String payload = new String(payloadBytes);
				synchronized (foundPayloads) {
					if (!foundPayloads.containsKey(payload)) {
						softly.fail("Received unexpected message %s", payload);
						return;
					}
				}
			}

			Instant timestamp;
			try { // Align the timestamps between the threads with a human-readable stall interval
				barrier.await();
				Thread.sleep(stallIntervalInMillis);
				timestamp = Instant.now();
				logger.info("Received message {} (size: {}) (timestamp: {})",
						StaticMessageHeaderAccessor.getId(message1), payloads.size(), timestamp);
			} catch (InterruptedException e) {
				logger.error("Interrupt received", e);
				return;
			} catch (BrokenBarrierException e) {
				logger.error("Unexpected barrier error", e);
				return;
			}

			for (byte[] payloadBytes : payloads) {
				String payload = new String(payloadBytes);
				synchronized (foundPayloads) {
					foundPayloads.put(payload, timestamp);
				}

				latch.countDown();
			}
		});

		messages.forEach(moduleOutputChannel::send);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		assertThat(foundPayloads).allSatisfy((payload, instant) -> {
			assertThat(instant).as("Did not receive message %s", payload).isNotNull();
			assertThat(foundPayloads.values()
					.stream()
					// Get "closest" messages with a margin of error of half the stalling interval
					.filter(instant1 -> Duration.between(instant, instant1).abs()
							.minusMillis(stallIntervalInMillis / 2).isNegative())
					.count())
					.as("Unexpected number of messages in parallel")
					.isEqualTo((long) consumerConcurrency * consumerProperties.getExtension().getBatchMaxSize());
		});

		String msgVpnName = (String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		List<String> txFlowsIds = sempV2Api.monitor().getMsgVpnQueueTxFlows(
				msgVpnName,
				queue0,
				Integer.MAX_VALUE, null, null, null)
				.getData()
				.stream()
				.map(MonitorMsgVpnQueueTxFlow::getFlowId)
				.map(String::valueOf)
				.collect(Collectors.toList());

		assertThat(txFlowsIds).hasSize(consumerConcurrency).allSatisfy(flowId -> retryAssert(() ->
				assertThat(sempV2Api.monitor()
						.getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null)
						.getData()
						.getAckedMsgCount())
						.as("Expected all flows to receive exactly %s messages", numMsgsPerFlow)
						.isEqualTo(numMsgsPerFlow)));

		retryAssert(() -> assertThat(txFlowsIds.stream()
				.map((ThrowingFunction<String, MonitorMsgVpnQueueTxFlowResponse>)
						flowId -> sempV2Api.monitor().getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null))
				.mapToLong(r -> r.getData().getAckedMsgCount())
				.sum())
				.as("Flows did not receive expected number of messages")
				.isEqualTo((long) numMsgsPerFlow * consumerConcurrency));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@ParameterizedTest(name = "[{index}] batchMode={0}")
	@ValueSource(booleans = {false, true})
	public void testPolledConsumerConcurrency(boolean batchMode,
											  SempV2Api sempV2Api,
											  SpringCloudStreamContext context,
											  SoftAssertions softly,
											  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		int consumerConcurrency = 2;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(consumerConcurrency);
		consumerProperties.setBatchMode(batchMode);
		if (!consumerProperties.isBatchMode()) {
			consumerProperties.getExtension().setBatchMaxSize(1);
		}
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		int numMsgsPerConsumer = 2;
		Set<Message<?>> messages = new HashSet<>();
		Map<String, Boolean> foundPayloads = new HashMap<>();
		for (int i = 0; i < numMsgsPerConsumer * consumerProperties.getExtension().getBatchMaxSize() *
				consumerConcurrency; i++) {
			String payload = "foo-" + i;
			foundPayloads.put(payload, false);
			messages.add(MessageBuilder.withPayload(payload.getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build());
		}

		context.binderBindUnbindLatency();

		String queue0 = binder.getConsumerQueueName(consumerBinding);

		messages.forEach(moduleOutputChannel::send);

		for (int i = 0; i < numMsgsPerConsumer * consumerConcurrency; i ++) {
			retryAssert(() -> assertThat(moduleInputChannel.poll(message1 -> {
				@SuppressWarnings("unchecked")
				List<byte[]> payloads = consumerProperties.isBatchMode() ? (List<byte[]>) message1.getPayload() :
						Collections.singletonList((byte[]) message1.getPayload());
				logger.info("Received message {} (size: {})", StaticMessageHeaderAccessor.getId(message1),
						payloads.size());
				for (byte[] payloadBytes : payloads) {
					String payload = new String(payloadBytes);
					synchronized (foundPayloads) {
						if (foundPayloads.containsKey(payload)) {
							foundPayloads.put(payload, true);
						} else {
							softly.fail("Received unexpected message %s", payload);
						}
					}
				}
			})).isTrue());
		}

		assertThat(foundPayloads).allSatisfy((key, value) -> assertThat(value)
				.as("Did not receive message %s", key)
				.isTrue());

		String msgVpnName = (String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor().getMsgVpnQueueTxFlows(
				msgVpnName,
				queue0,
				Integer.MAX_VALUE, null, null, null).getData();

		assertThat(txFlows).as("polled consumers don't support concurrency, expected it to be ignored")
				.hasSize(1);

		String flowId = String.valueOf(txFlows.iterator().next().getFlowId());
		retryAssert(() -> assertThat(sempV2Api.monitor()
				.getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null)
				.getData()
				.getAckedMsgCount())
				.as("Flow %s did not receive expected number of messages", flowId)
				.isEqualTo((long) numMsgsPerConsumer * consumerProperties.getExtension().getBatchMaxSize() *
						consumerConcurrency));

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testFailConsumerConcurrencyWithExclusiveQueue(SpringCloudStreamContext context) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.getExtension().setQueueAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail");
		} catch (ProvisioningException e) {
			assertThat(e).hasNoCause();
			assertThat(e.getMessage()).containsIgnoringCase("not supported");
			assertThat(e.getMessage()).containsIgnoringCase("concurrency > 1");
			assertThat(e.getMessage()).containsIgnoringCase("exclusive queue");
		}
	}

	@Test
	public void testFailPolledConsumerConcurrencyWithExclusiveQueue(SpringCloudStreamContext context) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(2);
		consumerProperties.getExtension().setQueueAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);

		try {
			Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail");
		} catch (ProvisioningException e) {
			assertThat(e).hasNoCause();
			assertThat(e.getMessage()).containsIgnoringCase("not supported");
			assertThat(e.getMessage()).containsIgnoringCase("concurrency > 1");
			assertThat(e.getMessage()).containsIgnoringCase("exclusive queue");
		}
	}

	@Test
	public void testFailConsumerConcurrencyWithAnonConsumerGroup(SpringCloudStreamContext context) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(2);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, null, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail");
		} catch (ProvisioningException e) {
			assertThat(e).hasNoCause();
			assertThat(e.getMessage()).containsIgnoringCase("not supported");
			assertThat(e.getMessage()).containsIgnoringCase("concurrency > 1");
			assertThat(e.getMessage()).containsIgnoringCase("anonymous consumer groups");
		}
	}

	@Test
	public void testFailPolledConsumerConcurrencyWithAnonConsumerGroup(SpringCloudStreamContext context) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(2);

		try {
			Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
					destination0, null, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail");
		} catch (ProvisioningException e) {
			assertThat(e).hasNoCause();
			assertThat(e.getMessage()).containsIgnoringCase("not supported");
			assertThat(e.getMessage()).containsIgnoringCase("concurrency > 1");
			assertThat(e.getMessage()).containsIgnoringCase("anonymous consumer groups");
		}
	}

	@Test
	public void testFailConsumerWithConcurrencyLessThan1(SpringCloudStreamContext context) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(0);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail");
		} catch (BinderException e) {
			assertThat(e).hasCauseInstanceOf(MessagingException.class);
			assertThat(e.getCause().getMessage()).containsIgnoringCase("concurrency must be greater than 0");
		}
	}

	@Test
	public void testFailConsumerWithConcurrencyGreaterThanMax(JCSMPSession jcsmpSession,
															  SempV2Api sempV2Api,
															  SpringCloudStreamContext context,
															  SoftAssertions softly) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		int concurrency = 2;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setConcurrency(concurrency);
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		String queue0 = SolaceProvisioningUtil
				.getQueueNames(destination0, group0, consumerProperties.getExtension(), false)
				.getConsumerGroupQueueName();

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queue0);
		try {
			long maxBindCount = 1;
			logger.info(String.format("Pre-provisioning queue %s with maxBindCount=%s", queue0, maxBindCount));
			jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
			sempV2Api.config()
					.updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().maxBindCount(maxBindCount), null);

			try {
				Binding<MessageChannel> consumerBinding = binder.bindConsumer(
						destination0, group0, moduleInputChannel, consumerProperties);
				consumerBinding.unbind();
				fail("Expected consumer provisioning to fail");
			} catch (BinderException e) {
				softly.assertThat(e).hasCauseInstanceOf(MessagingException.class);
				softly.assertThat(e.getCause().getMessage()).containsIgnoringCase("failed to get message consumer");
				softly.assertThat(e.getCause()).hasCauseInstanceOf(JCSMPErrorResponseException.class);
				softly.assertThat(((JCSMPErrorResponseException) e.getCause().getCause()).getSubcodeEx())
						.isEqualTo(JCSMPErrorResponseSubcodeEx.MAX_CLIENTS_FOR_QUEUE);
			}

			softly.assertThat(sempV2Api.monitor()
					.getMsgVpnQueue(vpnName, queue0, null)
					.getData()
					.getBindSuccessCount())
					.as("%s > %s means that there should have been at least one successful bind",
							concurrency, maxBindCount)
					.isGreaterThan(0);
			softly.assertThat(sempV2Api.monitor()
					.getMsgVpnQueueTxFlows(vpnName, queue0, Integer.MAX_VALUE, null, null, null)
					.getData()
					.size())
					.as("all consumer flows should have been closed if one of them failed to start")
					.isEqualTo(0);
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testConsumerProvisionWildcardDurableQueue(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String topic0Prefix = IntStream.range(0, 2)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining(context.getDestinationNameDelimiter()));
		String topic0 = topic0Prefix + IntStream.range(0, 3)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining("/"));
		String topic1Prefix = RandomStringUtils.randomAlphanumeric(30);
		String topic1 = topic1Prefix + RandomStringUtils.randomAlphanumeric(20);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setQueueAdditionalSubscriptions(new String[]{topic1Prefix + "*"});

		Message<?> message0 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Message<?> message1 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, topic1)
				.build();

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			producerBinding = binder.bindProducer(topic0, moduleOutputChannel, context.createProducerProperties(testInfo));
			consumerBinding = binder.bindConsumer(String.format("%s*/>", topic0Prefix),
					RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

			context.binderBindUnbindLatency();

			final CountDownLatch latch0 = new CountDownLatch(1);
			final CountDownLatch latch1 = new CountDownLatch(1);
			moduleInputChannel.subscribe(msg -> {
				logger.info(String.format("Received message %s", msg));
				Destination destination = msg.getHeaders().get(SolaceHeaders.DESTINATION, Destination.class);
				assertThat(destination).isNotNull();
				if (topic0.equals(destination.getName())) {
					latch0.countDown();
				} else if (topic1.equals(destination.getName())) {
					latch1.countDown();
				}
			});

			moduleOutputChannel.send(message0);
			moduleOutputChannel.send(message1);
			assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
		}
	}

	@Test
	public void testProducerProvisionWildcardDurableQueue(JCSMPSession jcsmpSession,
														  SpringCloudStreamContext context,
														  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

		String topic0 = String.format("%s*/>", IntStream.range(0, 2)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining(context.getDestinationNameDelimiter())));
		String topic1Prefix = RandomStringUtils.randomAlphanumeric(30);
		String topic1 = topic1Prefix + RandomStringUtils.randomAlphanumeric(20);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.setRequiredGroups(group0);
		producerProperties.getExtension().setQueueAdditionalSubscriptions(
				Collections.singletonMap(group0, new String[]{topic1Prefix + "*"}));

		Message<?> message0 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Message<?> message1 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, topic1)
				.build();

		Binding<MessageChannel> producerBinding = null;
		FlowReceiver flowReceiver = null;

		try {
			producerBinding = binder.bindProducer(topic0, moduleOutputChannel, producerProperties);
			context.binderBindUnbindLatency();

			final CountDownLatch latch0 = new CountDownLatch(1);
			final CountDownLatch latch1 = new CountDownLatch(1);

			ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
			consumerFlowProperties.setStartState(true);
			consumerFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(SolaceProvisioningUtil
					.getQueueName(topic0, group0, producerProperties.getExtension())));
			flowReceiver = jcsmpSession.createFlow(new XMLMessageListener() {
				@Override
				public void onReceive(BytesXMLMessage bytesXMLMessage) {
					logger.info(String.format("Received message %s", bytesXMLMessage));
					Destination destination = bytesXMLMessage.getDestination();
					assertThat(destination).isNotNull();
					if (topic0.equals(destination.getName())) {
						latch0.countDown();
					} else if (topic1.equals(destination.getName())) {
						latch1.countDown();
					}
				}

				@Override
				public void onException(JCSMPException e) {}
			}, consumerFlowProperties, new EndpointProperties());

			moduleOutputChannel.send(message0);
			moduleOutputChannel.send(message1);

			assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (flowReceiver != null) flowReceiver.close();
		}
	}

	@Test
	public void testPolledConsumerProvisionWildcardDurableQueue(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input",
				new BindingProperties());

		String topic0Prefix = IntStream.range(0, 2)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining(context.getDestinationNameDelimiter()));
		String topic0 = topic0Prefix + IntStream.range(0, 3)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining("/"));
		String topic1Prefix = RandomStringUtils.randomAlphanumeric(30);
		String topic1 = topic1Prefix + RandomStringUtils.randomAlphanumeric(20);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setQueueAdditionalSubscriptions(new String[]{topic1Prefix + "*"});

		Message<?> message0 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Message<?> message1 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, topic1)
				.build();

		Binding<MessageChannel> producerBinding = null;
		Binding<PollableSource<MessageHandler>> consumerBinding = null;

		try {
			producerBinding = binder.bindProducer(topic0, moduleOutputChannel, context.createProducerProperties(testInfo));
			consumerBinding = binder.bindPollableConsumer(String.format("%s*/>", topic0Prefix),
					RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

			context.binderBindUnbindLatency();

			moduleOutputChannel.send(message0);
			moduleOutputChannel.send(message1);

			AtomicBoolean received0 = new AtomicBoolean(false);
			AtomicBoolean received1 = new AtomicBoolean(false);
			for (int i = 0; i < 2; i++) {
				boolean gotMessage = false;
				for (int j = 0; !gotMessage && j < 100; j++) {
					gotMessage = moduleInputChannel.poll(msg -> {
						logger.info(String.format("Received message %s", msg));
						Destination destination = msg.getHeaders().get(SolaceHeaders.DESTINATION, Destination.class);
						assertThat(destination).isNotNull();
						if (topic0.equals(destination.getName())) {
							received0.set(true);
						} else if (topic1.equals(destination.getName())) {
							received1.set(true);
						}
					});
				}
				assertThat(gotMessage).isTrue();
			}
			assertThat(received0.get()).isTrue();
			assertThat(received1.get()).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
		}
	}

	@Test
	public void testAnonConsumerProvisionWildcardDurableQueue(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String topic0Prefix = IntStream.range(0, 2)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining(context.getDestinationNameDelimiter()));
		String topic0 = topic0Prefix + IntStream.range(0, 3)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining("/"));
		String topic1Prefix = RandomStringUtils.randomAlphanumeric(30);
		String topic1 = topic1Prefix + RandomStringUtils.randomAlphanumeric(20);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setQueueAdditionalSubscriptions(new String[]{topic1Prefix + "*"});

		Message<?> message0 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();
		Message<?> message1 = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, topic1)
				.build();

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			producerBinding = binder.bindProducer(topic0, moduleOutputChannel, context.createProducerProperties(testInfo));
			consumerBinding = binder.bindConsumer(String.format("%s*/>", topic0Prefix),
					null, moduleInputChannel, consumerProperties);

			context.binderBindUnbindLatency();

			final CountDownLatch latch0 = new CountDownLatch(1);
			final CountDownLatch latch1 = new CountDownLatch(1);
			moduleInputChannel.subscribe(msg -> {
				logger.info(String.format("Received message %s", msg));
				Destination destination = msg.getHeaders().get(SolaceHeaders.DESTINATION, Destination.class);
				assertThat(destination).isNotNull();
				if (topic0.equals(destination.getName())) {
					latch0.countDown();
				} else if (topic1.equals(destination.getName())) {
					latch1.countDown();
				}
			});

			moduleOutputChannel.send(message0);
			moduleOutputChannel.send(message1);
			assertThat(latch0.await(10, TimeUnit.SECONDS)).isTrue();
			assertThat(latch1.await(10, TimeUnit.SECONDS)).isTrue();
			TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
		}
	}

	@Test
	public void testConsumerProvisionWildcardErrorQueue(JCSMPSession jcsmpSession,
														SempV2Api sempV2Api,
														SpringCloudStreamContext context,
														TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0Prefix = IntStream.range(0, 2)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining(context.getDestinationNameDelimiter()));
		String destination0 = destination0Prefix + IntStream.range(0, 3)
				.mapToObj(i -> RandomStringUtils.randomAlphanumeric(10))
				.collect(Collectors.joining("/"));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setAutoBindErrorQueue(true);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			producerBinding = binder.bindProducer(destination0, moduleOutputChannel,
					context.createProducerProperties(testInfo));
			consumerBinding = binder.bindConsumer(String.format("%s*/>", destination0Prefix),
					RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

			context.binderBindUnbindLatency();

			moduleInputChannel.subscribe(message1 -> {
				throw new RuntimeException("Throwing expected exception!");
			});

			moduleOutputChannel.send(message);

			final ConsumerFlowProperties errorQueueFlowProperties = new ConsumerFlowProperties();
			errorQueueFlowProperties.setStartState(true);
			errorQueueFlowProperties.setEndpoint(JCSMPFactory.onlyInstance().createQueue(
					binder.getConsumerErrorQueueName(consumerBinding)));
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
							binder.getConsumerQueueName(consumerBinding),
							2, null, null, null)
					.getData();
			assertThat(enqueuedMessages).hasSize(0);
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
		}
	}

	@Test
	public void testConsumerProvisionIgnoreGroupNameInQueueName(JCSMPSession jcsmpSession,
																SempV2Api sempV2Api,
																SpringCloudStreamContext context,
																TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setUseGroupNameInQueueName(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		assertThat(queueName).doesNotContain('/' + group0 + '/');

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		assertThat(errorQueueName).contains('/' + group0 + '/');

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2,
						null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testPolledConsumerProvisionIgnoreGroupNameInQueueName(JCSMPSession jcsmpSession,
																	  SempV2Api sempV2Api,
																	  SpringCloudStreamContext context,
																	  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setUseGroupNameInQueueName(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		assertThat(queueName).doesNotContain('/' + group0 + '/');

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		assertThat(errorQueueName).contains('/' + group0 + '/');

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAnonConsumerProvisionIgnoreGroupNameInQueueName(JCSMPSession jcsmpSession,
																	SempV2Api sempV2Api,
																	SpringCloudStreamContext context,
																	TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setUseGroupNameInQueueName(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerProvisionIgnoreGroupNameInErrorQueueName(JCSMPSession jcsmpSession,
																	 SempV2Api sempV2Api,
																	 SpringCloudStreamContext context,
																	 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setUseGroupNameInErrorQueueName(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		assertThat(queueName).contains('/' + group0 + '/');

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		assertThat(errorQueueName).doesNotContain('/' + group0 + '/');

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAnonConsumerProvisionIgnoreGroupNameInErrorQueueName(JCSMPSession jcsmpSession,
																		 SempV2Api sempV2Api,
																		 SpringCloudStreamContext context,
																		 TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setUseGroupNameInErrorQueueName(false);
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerProvisionErrorQueueNameOverride(JCSMPSession jcsmpSession,
															SempV2Api sempV2Api,
															SpringCloudStreamContext context,
															TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setErrorQueueNameOverride(RandomStringUtils.randomAlphanumeric(100));
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		assertThat(queueName).contains('/' + group0 + '/');
		assertThat(queueName).endsWith('/' + destination0);

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		assertThat(errorQueueName).isEqualTo(consumerProperties.getExtension().getErrorQueueNameOverride());

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testAnonConsumerProvisionErrorQueueNameOverride(JCSMPSession jcsmpSession,
																SempV2Api sempV2Api,
																SpringCloudStreamContext context,
																TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(50);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.getExtension().setErrorQueueNameOverride(RandomStringUtils.randomAlphanumeric(100));
		consumerProperties.getExtension().setAutoBindErrorQueue(true);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		context.binderBindUnbindLatency();

		String queueName = binder.getConsumerQueueName(consumerBinding);
		assertThat(queueName).endsWith('/' + destination0);

		String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);
		assertThat(errorQueueName).isEqualTo(consumerProperties.getExtension().getErrorQueueNameOverride());

		CountDownLatch latch = new CountDownLatch(consumerProperties.getMaxAttempts());
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("Throwing expected exception!");
		});

		logger.info(String.format("Sending message to destination %s: %s", destination0, message));
		moduleOutputChannel.send(message);

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();

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

		List<MonitorMsgVpnQueueMsg> enqueuedMessages = sempV2Api.monitor()
				.getMsgVpnQueueMsgs((String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME), queueName, 2, null, null, null)
				.getData();
		assertThat(enqueuedMessages).hasSize(0);

		producerBinding.unbind();
		consumerBinding.unbind();
	}
}
