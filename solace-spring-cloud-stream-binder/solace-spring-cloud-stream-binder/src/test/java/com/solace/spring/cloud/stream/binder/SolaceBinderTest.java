package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.Topic;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
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
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.MimeTypeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigFileApplicationContextInitializer.class)
public class SolaceBinderTest
		extends PartitionCapableBinderTests<SolaceTestBinder, ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>> {

	@Autowired
	private SpringJCSMPFactory springJCSMPFactory;

	@Value("${test.fail.on.connection.exception:false}")
	private Boolean failOnConnectError;

	private JCSMPSession jcsmpSession;

	private static SolaceExternalResourceHandler externalResource = new SolaceExternalResourceHandler();


	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Override
	protected String getClassUnderTestName() {
		return this.getClass().getSimpleName();
	}

	@Override
	protected SolaceTestBinder getBinder() throws Exception {
		if (testBinder == null) {
			logger.info(String.format("Getting new %s instance", SolaceTestBinder.class.getSimpleName()));
			jcsmpSession = externalResource.assumeAndGetActiveSession(springJCSMPFactory, failOnConnectError);
			testBinder = new SolaceTestBinder(jcsmpSession);
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<SolaceProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new SolaceProducerProperties());
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}

	// NOT YET SUPPORTED ---------------------------------
	@Override
	public void testPartitionedModuleJava() {
		Assume.assumeTrue("Partitioning not currently supported", false);
	}

	@Override
	public void testPartitionedModuleSpEL() {
		Assume.assumeTrue("Partitioning not currently supported", false);
	}
	// ---------------------------------------------------

	@Test
	public void testSendAndReceiveBad() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, "testSendAndReceiveBad", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.build();

		binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(3);
		moduleInputChannel.subscribe(message1 -> {
			latch.countDown();
			throw new RuntimeException("bad");
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testProducerErrorChannel() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination0EC = String.format("%s%serrors", destination0, getDestinationNameDelimiter());

		ExtendedProducerProperties<SolaceProducerProperties> producerProps = createProducerProperties();
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
	public void testPolledConsumer() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
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
	public void testPolledConsumerRequeue() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setRequeueRejected(true);
		Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(
				destination0, "testPolledConsumerRequeue", moduleInputChannel, consumerProperties);

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

		gotMessage = moduleInputChannel.poll(message1 -> logger.info(String.format("Received message %s", message1)));
		assertThat(gotMessage).isTrue();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testFailProducerProvisioningOnRequiredQueuePropertyChange() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailProducerProvisioningOnRequiredQueuePropertyChange";

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);

		logger.info(String.format("Pre-provisioning queue %s with AccessType %s to conflict with defaultAccessType %s",
				queue.getName(), endpointProperties.getAccessType(), defaultAccessType));
		jcsmpSession.provision(queue, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		Binding<MessageChannel> producerBinding;
		try {
			ExtendedProducerProperties<SolaceProducerProperties> bindingProducerProperties = createProducerProperties();
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
	public void testFailConsumerProvisioningOnQueuePropertyChange() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerProvisioningOnQueuePropertyChange";

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		Queue queue = JCSMPFactory.onlyInstance().createQueue(destination0 + getDestinationNameDelimiter() + group0);

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
	public void testFailConsumerProvisioningOnDmqPropertyChange() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testFailConsumerProvisioningOnDmqPropertyChange";

		int defaultAccessType = createConsumerProperties().getExtension().getQueueAccessType();
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType((defaultAccessType + 1) % 2);
		String dmqName = destination0 + getDestinationNameDelimiter() + group0 + getDestinationNameDelimiter() + "dmq";
		Queue dmq = JCSMPFactory.onlyInstance().createQueue(dmqName);

		logger.info(String.format("Pre-provisioning DMQ %s with AccessType %s to conflict with defaultAccessType %s",
				dmq.getName(), endpointProperties.getAccessType(), defaultAccessType));
		jcsmpSession.provision(dmq, endpointProperties, JCSMPSession.WAIT_FOR_CONFIRM);

		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());
		Binding<MessageChannel> consumerBinding;
		try {
			ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
			consumerProperties.getExtension().setAutoBindDmq(true);
			consumerBinding = binder.bindConsumer(
					destination0, group0, moduleInputChannel, consumerProperties);
			consumerBinding.unbind();
			fail("Expected consumer provisioning to fail due to DMQ property change");
		} catch (ProvisioningException e) {
			assertThat(e).hasCauseInstanceOf(PropertyMismatchException.class);
			logger.info(String.format("Successfully threw a %s exception with cause %s",
					ProvisioningException.class.getSimpleName(), PropertyMismatchException.class.getSimpleName()));
		} finally {
			jcsmpSession.deprovision(dmq, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
		}
	}

	@Test
	public void testConsumerAdditionalSubscriptions() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel0 = createBindableChannel("output0", new BindingProperties());
		DirectChannel moduleOutputChannel1 = createBindableChannel("output1", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination1 = "some-destl";
		String wildcardDestination1 = destination1.substring(0, destination1.length() - 1) + "*";

		Binding<MessageChannel> producerBinding0 = binder.bindProducer(
				destination0, moduleOutputChannel0, createProducerProperties());
		Binding<MessageChannel> producerBinding1 = binder.bindProducer(
				destination1, moduleOutputChannel1, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension()
				.setQueueAdditionalSubscriptions(new String[]{wildcardDestination1, "some-random-sub"});

		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, "testConsumerAdditionalSubscriptions", moduleInputChannel, consumerProperties);

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
	public void testProducerAdditionalSubscriptions() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel0 = createBindableChannel("output0", new BindingProperties());
		DirectChannel moduleOutputChannel1 = createBindableChannel("output1", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination1 = "some-destl";
		String wildcardDestination1 = destination1.substring(0, destination1.length() - 1) + "*";
		String group0 = "testProducerAdditionalSubscriptions";

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		Map<String,String[]> groupsAdditionalSubs = new HashMap<>();
		groupsAdditionalSubs.put(group0, new String[]{wildcardDestination1});
		producerProperties.setRequiredGroups(group0);
		producerProperties.getExtension().setQueueAdditionalSubscriptions(groupsAdditionalSubs);

		Binding<MessageChannel> producerBinding0 = binder.bindProducer(
				destination0, moduleOutputChannel0, producerProperties);
		Binding<MessageChannel> producerBinding1 = binder.bindProducer(
				destination1, moduleOutputChannel1, createProducerProperties());

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
		consumerProperties.getExtension().setAddDurableQueueSubscription(false);

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
	public void testConsumerAddDurableQueueSubscription() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerAddDurableQueueSubscription";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isAddDurableQueueSubscription()).isTrue();
		consumerProperties.getExtension().setAddDurableQueueSubscription(false);

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
	public void testProducerAddDurableQueueSubscription() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testProducerAddDurableQueueSubscription";

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
		assertThat(producerProperties.getExtension().isAddDurableQueueSubscription()).isTrue();
		producerProperties.getExtension().setAddDurableQueueSubscription(false);
		producerProperties.setRequiredGroups(group0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		consumerProperties.getExtension().setProvisionDurableQueue(false);
		consumerProperties.getExtension().setAddDurableQueueSubscription(false);

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
	public void testPolledConsumerAddDurableQueueSubscription() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerAddDurableQueueSubscription";

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isAddDurableQueueSubscription()).isTrue();
		consumerProperties.getExtension().setAddDurableQueueSubscription(false);

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
	public void testAnonConsumerAddDurableQueueSubscription() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
		assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
		consumerProperties.getExtension().setAddDurableQueueSubscription(false); // Expect this parameter to do nothing

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
