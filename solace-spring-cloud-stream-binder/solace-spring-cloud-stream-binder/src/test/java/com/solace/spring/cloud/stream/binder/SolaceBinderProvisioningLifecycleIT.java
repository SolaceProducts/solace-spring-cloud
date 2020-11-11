package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredSpringRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.test.util.ThrowingFunction;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlow;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueTxFlowResponse;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.solace.spring.cloud.stream.binder.test.util.ValuePoller.poll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.is;

/**
 * All tests which modify the default provisioning lifecycle.
 */
@RunWith(InheritedTestsFilteredSpringRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderProvisioningLifecycleIT extends SolaceBinderITBase {
	@Test
	public void testConsumerProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerProvisionDurableQueue";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);

		Binding<MessageChannel> producerBinding = null;
		Binding<MessageChannel> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning queue %s with Permission %s", queue.getName(), endpointProperties.getPermission()));
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

			Message<?> message = MessageBuilder.withPayload("foo".getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build();

			binderBindUnbindLatency();

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
	public void testProducerProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testProducerProvisionDurableQueue";

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		assertThat(producerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		producerProperties.getExtension().setProvisionDurableQueue(false);
		producerProperties.setRequiredGroups(group0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setProvisionDurableQueue(false);
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
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

			binderBindUnbindLatency();

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
	public void testPolledConsumerProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testPolledConsumerProvisionDurableQueue";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setPermission(EndpointProperties.PERMISSION_MODIFY_TOPIC);

		Binding<MessageChannel> producerBinding = null;
		Binding<PollableSource<MessageHandler>> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning queue %s with Permission %s", queue.getName(), endpointProperties.getPermission()));
			jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

			producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
			consumerBinding = binder.bindPollableConsumer(destination0, group0, moduleInputChannel, consumerProperties);

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
		} finally {
			if (producerBinding != null) producerBinding.unbind();
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testAnonConsumerProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionDurableQueue(false); // Expect this parameter to do nothing

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

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
	public void testFailConsumerProvisioningOnDisablingProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerProvisioningOnDisablingProvisionDurableQueue";

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailProducerProvisioningOnDisablingProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailProducerProvisioningOnDisablingProvisionDurableQueue";

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
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
	public void testFailPolledConsumerProvisioningOnDisablingProvisionDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailPolledConsumerProvisioningOnDisablingProvisionDurableQueue";

		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testConsumerProvisionDmq() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerProvisionDmq";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDmq()).isTrue();
		consumerProperties.getExtension().setProvisionDmq(false);
		consumerProperties.getExtension().setAutoBindDmq(true);

		String dmqName = destination0 + getDestinationNameDelimiter() + group0 + getDestinationNameDelimiter() + "dmq";
		Queue dmq = JCSMPFactory.onlyInstance().createQueue(dmqName);

		Binding<MessageChannel> consumerBinding = null;

		try {
			logger.info(String.format("Pre-provisioning DMQ %s", dmq.getName()));
			jcsmpSession.provision(dmq, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);

			consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);
			binderBindUnbindLatency();
		} finally {
			if (consumerBinding != null) consumerBinding.unbind();
			jcsmpSession.deprovision(dmq, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testFailConsumerProvisioningOnDisablingProvisionDmq() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerProvisioningOnDisablingProvisionDmq";

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDmq()).isTrue();
		consumerProperties.getExtension().setProvisionDmq(false);
		consumerProperties.getExtension().setAutoBindDmq(true);

		try {
			Binding<MessageChannel> consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to missing DMQ");
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
	public void testConsumerProvisionSubscriptionsToDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerProvisionSubscriptionsToDurableQueue";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
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
	public void testProducerProvisionSubscriptionsToDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testProducerProvisionSubscriptionsToDurableQueue";

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		assertThat(producerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		producerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);
		producerProperties.setRequiredGroups(group0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setProvisionDurableQueue(false);
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, producerProperties);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, group0, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(message1 -> {
			logger.info(String.format("Received message %s", message1));
			latch.countDown();
		});

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
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
	public void testPolledConsumerProvisionSubscriptionsToDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerProvisionSubscriptionsToDurableQueue";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(destination0, group0,
				moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		logger.info(String.format("Checking that there is no subscription is on group %s of destination %s", group0, destination0));
		moduleOutputChannel.send(message);

		for (int i = 0; i < 10; i++) {
			TimeUnit.SECONDS.sleep(1);
			moduleInputChannel.poll(message1 -> fail(String.format("Didn't expect to receive message %s", message1)));
		}

		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);
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
	public void testAnonConsumerProvisionSubscriptionsToDurableQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(false); // Expect this parameter to do nothing

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination0, null, moduleInputChannel, consumerProperties);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

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
	public void testConsumerConcurrency() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerConcurrency";
		String queue0 = destination0 + getDestinationNameDelimiter() + group0;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		int consumerConcurrency = 3;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(consumerConcurrency);
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		int numMsgsPerFlow = 10;
		Set<Message<?>> messages = new HashSet<>();
		Map<String, Instant> foundPayloads = new HashMap<>();
		for (int i = 0; i < numMsgsPerFlow * consumerConcurrency; i++) {
			String payload = "foo-" + i;
			foundPayloads.put(payload, null);
			messages.add(MessageBuilder.withPayload(payload.getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build());
		}

		binderBindUnbindLatency();

		final long stallIntervalInMillis = 100;
		final CyclicBarrier barrier = new CyclicBarrier(consumerConcurrency);
		final CountDownLatch latch = new CountDownLatch(numMsgsPerFlow * consumerConcurrency);
		moduleInputChannel.subscribe(message1 -> {
			String payload = new String((byte[]) message1.getPayload());
			synchronized (foundPayloads) {
				if (!foundPayloads.containsKey(payload)) {
					logger.error(String.format("Received unexpected message %s", payload));
					return;
				}
			}

			Instant timestamp;
			try { // Align the timestamps between the threads with a human-readable stall interval
				barrier.await();
				Thread.sleep(stallIntervalInMillis);
				timestamp = Instant.now();
				logger.info(String.format("Received message %s", payload));
			} catch (InterruptedException e) {
				logger.error("Interrupt received", e);
				return;
			} catch (BrokenBarrierException e) {
				logger.error("Unexpected barrier error", e);
				return;
			}

			synchronized (foundPayloads) {
				foundPayloads.put(payload, timestamp);
			}

			latch.countDown();
		});

		messages.forEach(moduleOutputChannel::send);

		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		for (Map.Entry<String, Instant> payloadInstant : foundPayloads.entrySet()) {
			String payload = payloadInstant.getKey();
			Instant instant = payloadInstant.getValue();
			assertThat(instant).as("Did not receive message %s", payload).isNotNull();
			long numMsgsInParallel = foundPayloads.values()
					.stream()
					// Get "closest" messages with a margin of error of half the stalling interval
					.filter(instant1 -> Duration.between(instant, instant1).abs().getNano() <=
							TimeUnit.MILLISECONDS.toNanos(stallIntervalInMillis) / 2)
					.count();
			assertThat(numMsgsInParallel).isEqualTo(consumerConcurrency);
		}

		String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		List<String> txFlowsIds = sempV2Api.monitor().getMsgVpnQueueTxFlows(
				msgVpnName,
				queue0,
				Integer.MAX_VALUE, null, null, null)
				.getData()
				.stream()
				.map(MonitorMsgVpnQueueTxFlow::getFlowId)
				.map(String::valueOf)
				.collect(Collectors.toList());

		assertThat(txFlowsIds).hasSize(consumerConcurrency);

		for (String flowId : txFlowsIds) {
			assertThat(poll(() -> sempV2Api.monitor().getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null).getData().getAckedMsgCount())
							.until(is((long) numMsgsPerFlow))
							.execute()
							.get())
					.as("Expected all flows to receive exactly %s messages", numMsgsPerFlow)
					.isEqualTo(numMsgsPerFlow);
		}

		assertThat(poll(() -> txFlowsIds.stream()
								.map((ThrowingFunction<String, MonitorMsgVpnQueueTxFlowResponse>)
										flowId -> sempV2Api.monitor().getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null))
								.mapToLong(r -> r.getData().getAckedMsgCount())
								.sum())
						.until(is((long) numMsgsPerFlow * consumerConcurrency))
						.execute()
						.get())
				.as("Flows did not receive expected number of messages")
				.isEqualTo(numMsgsPerFlow * consumerConcurrency);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testPolledConsumerConcurrency() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testPolledConsumerConcurrency";
		String queue0 = destination0 + getDestinationNameDelimiter() + group0;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		int consumerConcurrency = 2;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(consumerConcurrency);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, group0, moduleInputChannel, consumerProperties);

		int numMsgsPerFlow = 2;
		Set<Message<?>> messages = new HashSet<>();
		Map<String, Boolean> foundPayloads = new HashMap<>();
		for (int i = 0; i < numMsgsPerFlow * consumerConcurrency; i++) {
			String payload = "foo-" + i;
			foundPayloads.put(payload, false);
			messages.add(MessageBuilder.withPayload(payload.getBytes())
					.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
					.build());
		}

		binderBindUnbindLatency();

		messages.forEach(moduleOutputChannel::send);

		for (int i = 0; i < 100; i++) {
			moduleInputChannel.poll(message1 -> {
				String payload = new String((byte[]) message1.getPayload());
				logger.info(String.format("Received message %s", payload));
				synchronized (foundPayloads) {
					if (foundPayloads.containsKey(payload)) {
						foundPayloads.put(payload, true);
					} else {
						logger.error(String.format("Received unexpected message %s", payload));
					}
				}
			});
		}

		foundPayloads.forEach((key, value) ->
				assertThat(value).as("Did not receive message %s", key).isTrue());

		String msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		List<MonitorMsgVpnQueueTxFlow> txFlows = sempV2Api.monitor().getMsgVpnQueueTxFlows(
				msgVpnName,
				queue0,
				Integer.MAX_VALUE, null, null, null).getData();

		assertThat(txFlows).as("polled consumers don't support concurrency, expected it to be ignored")
				.hasSize(1);

		String flowId = String.valueOf(txFlows.iterator().next().getFlowId());
		assertThat(poll(() -> sempV2Api.monitor().getMsgVpnQueueTxFlow(msgVpnName, queue0, flowId, null).getData().getAckedMsgCount())
						.until(is((long) numMsgsPerFlow * consumerConcurrency))
						.execute()
						.get())
				.as("Flow %s did not receive expected number of messages", flowId)
				.isEqualTo(numMsgsPerFlow * consumerConcurrency);

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testFailConsumerConcurrencyWithExclusiveQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerConcurrencyWithExclusiveQueue";

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailPolledConsumerConcurrencyWithExclusiveQueue() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerConcurrencyWithExclusiveQueue";

		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailConsumerConcurrencyWithAnonConsumerGroup() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailPolledConsumerConcurrencyWithAnonConsumerGroup() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailConsumerWithConcurrencyLessThan1() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerWithConcurrencyLessThan1";

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
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
	public void testFailConsumerWithConcurrencyGreaterThanMax() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerWithConcurrencyGreaterThanMax";
		String queue0 = destination0 + getDestinationNameDelimiter() + group0;

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		int concurrency = 2;
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.setConcurrency(concurrency);
		consumerProperties.getExtension().setProvisionDurableQueue(false);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);

		Queue queue = JCSMPFactory.onlyInstance().createQueue(queue0);
		try {
			long maxBindCount = 1;
			logger.info(String.format("Pre-provisioning queue %s with maxBindCount=%s", queue0, maxBindCount));
			jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
			sempV2Api.config()
					.updateMsgVpnQueue(vpnName, queue0, new ConfigMsgVpnQueue().maxBindCount(maxBindCount), null);

			SoftAssertions softly = new SoftAssertions();
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

			softly.assertAll();
		} finally {
			jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}
}
