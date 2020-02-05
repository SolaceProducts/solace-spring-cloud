package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredSpringRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * All tests which modify the default provisioning lifecycle.
 */
@RunWith(InheritedTestsFilteredSpringRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderProvisioningLifecycleTest extends SolaceBinderTestBase {
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
}
