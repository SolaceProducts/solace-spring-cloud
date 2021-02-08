package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solacesystems.jcsmp.EndpointProperties;
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
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

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
		String queueName = destination0 + getDestinationNameDelimiter() + group0;

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

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
				.getData())
				.hasSize(0);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(0);

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
		String queueName = destination0 + getDestinationNameDelimiter() + group0;

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

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
				.getData())
				.hasSize(0);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(0);

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
		String queueName = destination0 + getDestinationNameDelimiter() + group0;

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

		// Give some time for the message to actually ack off the original queue
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		assertThat(sempV2Api.monitor()
				.getMsgVpnQueueMsgs(vpnName, queueName, 1, null, null, null)
				.getData())
				.hasSize(0);
		assertThat(sempV2Api.monitor()
				.getMsgVpnQueue(vpnName, queueName, null)
				.getData()
				.getRedeliveredMsgCount())
				.isEqualTo(0);

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
