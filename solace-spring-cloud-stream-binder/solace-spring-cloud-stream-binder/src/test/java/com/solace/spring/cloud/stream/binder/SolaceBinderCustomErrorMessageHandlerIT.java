package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.SolaceErrorMessageHandler;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.XMLMessage;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

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
	public void testOverrideErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testOverrideErrorMessageHandler";
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
			applyErrorMessageAssertions(msg, softly);
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
	public void testOverrideRetryableErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testOverrideRetryableErrorMessageHandler";
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
			applyErrorMessageAssertions(msg, softly);
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
	public void testOverridePollableErrorMessageHandler() throws Exception {
		SolaceTestBinder binder = getBinder();

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String group0 = "testOverridePollableErrorMessageHandler";
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
			applyErrorMessageAssertions(msg, softly);
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

	private void applyErrorMessageAssertions(Message<?> errorMessage, SoftAssertions softAssertions) {
		softAssertions.assertThat(errorMessage).isInstanceOf(ErrorMessage.class);
		softAssertions.assertThat(((ErrorMessage) errorMessage).getOriginalMessage()).isNotNull();
		softAssertions.assertThat(((ErrorMessage) errorMessage).getPayload()).isNotNull();
		softAssertions.assertThat(errorMessage.getHeaders()
				.get(SolaceBinderHeaders.RAW_MESSAGE)).isInstanceOf(XMLMessage.class);
	}
}
