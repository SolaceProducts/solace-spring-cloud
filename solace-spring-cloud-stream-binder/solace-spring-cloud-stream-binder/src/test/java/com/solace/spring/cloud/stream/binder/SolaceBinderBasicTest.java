package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueue;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPInterruptedException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.PropertyMismatchException;
import com.solacesystems.jcsmp.Queue;
import org.assertj.core.api.SoftAssertions;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PollableSource;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.solace.spring.cloud.stream.binder.test.util.ValuePoller.poll;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.is;

/**
 * Runs all basic Spring Cloud Stream Binder functionality tests
 * inherited by {@link PartitionCapableBinderTests PartitionCapableBinderTests}
 * along with basic tests specific to the Solace Spring Cloud Stream Binder.
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigFileApplicationContextInitializer.class)
public class SolaceBinderBasicTest extends SolaceBinderTestBase {
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
	public void testConsumerReconnect() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerReconnect";
		String queue0 = destination0 + getDestinationNameDelimiter() + group0;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		binderBindUnbindLatency();

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		moduleInputChannel.subscribe(message1 -> {
			numMsgsConsumed.incrementAndGet();
			String payload = new String((byte[]) message1.getPayload());
			logger.info(String.format("Received message %s", payload));
			uniquePayloadsReceived.add(payload);
		});

		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Integer> future = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!Thread.currentThread().isInterrupted()) {
				String payload = "foo-" + numMsgsSent;
				Message<?> message = MessageBuilder.withPayload(payload.getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build();
				logger.info(String.format("Sending message %s", payload));
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
		executor.awaitTermination(20, TimeUnit.SECONDS);
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

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(queueState.getDisabledBindFailureCount()).isGreaterThan(0);
		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		// -2 margin of error. Redelivered messages might be untracked if it's consumer was shutdown before it could
		// be added to the consumed msg count.
		softly.assertThat(numMsgsConsumed.get() - queueState.getRedeliveredMsgCount())
				.isBetween((long)numMsgsSent-2, (long)numMsgsSent);
		softly.assertAll();

		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testConsumerRebind() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testConsumerUnbind";
		String queue0 = destination0 + getDestinationNameDelimiter() + group0;

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, group0, moduleInputChannel, createConsumerProperties());

		binderBindUnbindLatency();

		final AtomicInteger numMsgsConsumed = new AtomicInteger(0);
		final Set<String> uniquePayloadsReceived = new HashSet<>();
		moduleInputChannel.subscribe(message1 -> {
			numMsgsConsumed.incrementAndGet();
			String payload = new String((byte[]) message1.getPayload());
			logger.info(String.format("Received message %s", payload));
			uniquePayloadsReceived.add(payload);
		});

		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Integer> future = executor.submit(() -> {
			int numMsgsSent = 0;
			while (!Thread.currentThread().isInterrupted()) {
				String payload = "foo-" + numMsgsSent;
				Message<?> message = MessageBuilder.withPayload(payload.getBytes())
						.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
						.build();
				logger.info(String.format("Sending message %s", payload));
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
		executor.awaitTermination(20, TimeUnit.SECONDS);
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

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(uniquePayloadsReceived.size()).isEqualTo(numMsgsSent);
		// -2 margin of error. Redelivered messages might be untracked if it's consumer was shutdown before it could
		// be added to the consumed msg count.
		softly.assertThat(numMsgsConsumed.get() - redeliveredMsgs).isBetween((long)numMsgsSent-2, (long)numMsgsSent);
		softly.assertAll();

		producerBinding.unbind();
		consumerBinding.unbind();
	}
}
