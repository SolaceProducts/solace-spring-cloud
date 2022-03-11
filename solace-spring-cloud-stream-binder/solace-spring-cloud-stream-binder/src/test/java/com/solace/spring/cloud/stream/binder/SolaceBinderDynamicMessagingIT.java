package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * All tests regarding messaging which use a dynamic configuration on a message-by-message basis.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderDynamicMessagingIT {

	@Test
	public void testTargetDestination(SpringCloudStreamContext context, SoftAssertions softly,
									  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel0 = context.createBindableChannel("input0", new BindingProperties());
		DirectChannel moduleInputChannel1 = context.createBindableChannel("input1", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		String destination1 = RandomStringUtils.randomAlphanumeric(10);
		String group0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding0 = binder.bindConsumer(
				destination0, group0, moduleInputChannel0, context.createConsumerProperties());
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(
				destination1, group0, moduleInputChannel1, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, destination1)
				.build();

		context.binderBindUnbindLatency();

		Function<CountDownLatch, MessageHandler> msgHandlerFactory = latch -> m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		};

		final CountDownLatch latch0 = new CountDownLatch(1);
		moduleInputChannel0.subscribe(msgHandlerFactory.apply(latch0));

		final CountDownLatch latch1 = new CountDownLatch(1);
		moduleInputChannel1.subscribe(msgHandlerFactory.apply(latch1));

		moduleOutputChannel.send(message);

		softly.assertThat(latch0.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding0.getBindingName()).isFalse();
		softly.assertThat(latch1.await(10, TimeUnit.SECONDS))
				.as("Expected %s to get msg", consumerBinding1.getBindingName()).isTrue();

		producerBinding.unbind();
		consumerBinding0.unbind();
		consumerBinding1.unbind();
	}

	@Test
	public void testTargetDestinationWithEmptyString(SpringCloudStreamContext context, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, "")
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testTargetDestinationWithWhitespace(SpringCloudStreamContext context, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, "   ")
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testTargetDestinationWithNull(SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, context.createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, null)
				.build();

		context.binderBindUnbindLatency();

		final CountDownLatch latch = new CountDownLatch(1);
		moduleInputChannel.subscribe(m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		});

		moduleOutputChannel.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		producerBinding.unbind();
		consumerBinding.unbind();
	}

	@Test
	public void testTargetDestinationWithNonString(SpringCloudStreamContext context, TestInfo testInfo)
			throws Exception {
		SolaceTestBinder binder = context.getBinder();

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, 1)
				.build();

		context.binderBindUnbindLatency();

		try {
			moduleOutputChannel.send(message);
			fail("Expected message publish to fail");
		} catch (MessagingException e) {
			assertThat(e).getCause().isInstanceOf(IllegalArgumentException.class);
			assertThat(e).getCause().hasMessageContaining(BinderHeaders.TARGET_DESTINATION);
		}

		producerBinding.unbind();
	}
}
