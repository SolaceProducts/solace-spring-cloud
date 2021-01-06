package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderHeaders;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/**
 * All tests regarding messaging which use a dynamic configuration on a message-by-message basis.
 */
@RunWith(InheritedTestsFilteredRunner.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderDynamicMessagingIT extends SolaceBinderITBase {
	@Test
	public void testTargetDestination() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel0 = createBindableChannel("input0", new BindingProperties());
		DirectChannel moduleInputChannel1 = createBindableChannel("input1", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination1 = String.format("foo%s1", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding0 = binder.bindConsumer(
				destination0, "testTargetDestination", moduleInputChannel0, createConsumerProperties());
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(
				destination1, "testTargetDestination", moduleInputChannel1, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, destination1)
				.build();

		binderBindUnbindLatency();

		Function<CountDownLatch, MessageHandler> msgHandlerFactory = latch -> m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		};

		final CountDownLatch latch0 = new CountDownLatch(1);
		moduleInputChannel0.subscribe(msgHandlerFactory.apply(latch0));

		final CountDownLatch latch1 = new CountDownLatch(1);
		moduleInputChannel1.subscribe(msgHandlerFactory.apply(latch1));

		moduleOutputChannel.send(message);

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(latch0.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding0.getBindingName()).isFalse();
		softly.assertThat(latch1.await(10, TimeUnit.SECONDS))
				.as("Expected %s to get msg", consumerBinding1.getBindingName()).isTrue();
		softly.assertAll();

		producerBinding.unbind();
		consumerBinding0.unbind();
		consumerBinding1.unbind();
	}

	@Test
	public void testTargetDestinationWithStaticPrefix() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel0 = createBindableChannel("input0", new BindingProperties());
		DirectChannel moduleInputChannel1 = createBindableChannel("input1", new BindingProperties());
		DirectChannel moduleInputChannel2 = createBindableChannel("input2", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination1 = String.format("foo%s1", getDestinationNameDelimiter());
		String prefix0 = String.format("prefix%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties0 = createConsumerProperties();
		consumerProperties0.getExtension().setPrefix(prefix0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties2 = createConsumerProperties();
		consumerProperties2.getExtension().setPrefix(prefix0);

		Binding<MessageChannel> consumerBinding0 = binder.bindConsumer(
				destination0, "testTargetDestinationWithStaticPrefix", moduleInputChannel0, consumerProperties0);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(
				destination1, "testTargetDestinationWithStaticPrefix", moduleInputChannel1, createConsumerProperties());
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(
				destination1, "testTargetDestinationWithStaticPrefix", moduleInputChannel2, consumerProperties2);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, destination1)
				.build();

		binderBindUnbindLatency();

		Function<CountDownLatch, MessageHandler> msgHandlerFactory = latch -> m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		};

		final CountDownLatch latch0 = new CountDownLatch(1);
		moduleInputChannel0.subscribe(msgHandlerFactory.apply(latch0));

		final CountDownLatch latch1 = new CountDownLatch(1);
		moduleInputChannel1.subscribe(msgHandlerFactory.apply(latch1));

		final CountDownLatch latch2 = new CountDownLatch(1);
		moduleInputChannel2.subscribe(msgHandlerFactory.apply(latch2));

		moduleOutputChannel.send(message);

		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(latch0.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding0.getBindingName()).isFalse();
		softly.assertThat(latch1.await(10, TimeUnit.SECONDS))
				.as("Expected %s to get msg", consumerBinding1.getBindingName()).isTrue();
		softly.assertThat(latch2.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding2.getBindingName()).isFalse();
		softly.assertAll();

		producerBinding.unbind();
		consumerBinding0.unbind();
		consumerBinding1.unbind();
		consumerBinding2.unbind();
	}

	@Test
	public void testTargetDestinationWithPrefixAndStaticPrefix() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel0 = createBindableChannel("input0", new BindingProperties());
		DirectChannel moduleInputChannel1 = createBindableChannel("input1", new BindingProperties());
		DirectChannel moduleInputChannel2 = createBindableChannel("input2", new BindingProperties());
		DirectChannel moduleInputChannel3 = createBindableChannel("input3", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());
		String destination1 = String.format("foo%s1", getDestinationNameDelimiter());
		String prefix0 = String.format("prefix%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties0 = createConsumerProperties();
		consumerProperties0.getExtension().setPrefix(prefix0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties2 = createConsumerProperties();
		consumerProperties2.getExtension().setPrefix(prefix0);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties3 = createConsumerProperties();
		consumerProperties2.getExtension().setPrefix(prefix0 + prefix0);

		Binding<MessageChannel> consumerBinding0 = binder.bindConsumer(
				destination0, "testTargetDestinationWithPrefixAndStaticPrefix", moduleInputChannel0, consumerProperties0);
		Binding<MessageChannel> consumerBinding1 = binder.bindConsumer(
				destination1, "testTargetDestinationWithPrefixAndStaticPrefix", moduleInputChannel1, createConsumerProperties());
		Binding<MessageChannel> consumerBinding2 = binder.bindConsumer(
				destination1, "testTargetDestinationWithPrefixAndStaticPrefix", moduleInputChannel2, consumerProperties2);
		Binding<MessageChannel> consumerBinding3 = binder.bindConsumer(
				destination1, "testTargetDestinationWithPrefixAndStaticPrefix", moduleInputChannel3, consumerProperties3);

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, prefix0 + destination1)
				.build();

		binderBindUnbindLatency();

		Function<CountDownLatch, MessageHandler> msgHandlerFactory = latch -> m -> {
			assertThat(m.getHeaders()).doesNotContainKey(BinderHeaders.TARGET_DESTINATION);
			latch.countDown();
		};

		final CountDownLatch latch0 = new CountDownLatch(1);
		moduleInputChannel0.subscribe(msgHandlerFactory.apply(latch0));

		final CountDownLatch latch1 = new CountDownLatch(1);
		moduleInputChannel1.subscribe(msgHandlerFactory.apply(latch1));

		final CountDownLatch latch2 = new CountDownLatch(1);
		moduleInputChannel2.subscribe(msgHandlerFactory.apply(latch2));

		final CountDownLatch latch3 = new CountDownLatch(1);
		moduleInputChannel3.subscribe(msgHandlerFactory.apply(latch3));

		moduleOutputChannel.send(message);


		SoftAssertions softly = new SoftAssertions();
		softly.assertThat(latch0.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding0.getBindingName()).isFalse();
		softly.assertThat(latch1.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding1.getBindingName()).isFalse();
		softly.assertThat(latch2.await(10, TimeUnit.SECONDS))
				.as("Expected %s to get msg", consumerBinding2.getBindingName()).isTrue();
		softly.assertThat(latch3.await(10, TimeUnit.SECONDS))
				.as("Didn't expect %s to get msg", consumerBinding2.getBindingName()).isFalse();

		producerBinding.unbind();
		consumerBinding0.unbind();
		consumerBinding1.unbind();
		consumerBinding2.unbind();
		consumerBinding3.unbind();
	}

	@Test
	public void testTargetDestinationWithEmptyString() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, "testTargetDestinationWithEmptyString", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, "")
				.build();

		binderBindUnbindLatency();

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
	public void testTargetDestinationWithWhitespace() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, "testTargetDestinationWithWhitespace", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, "   ")
				.build();

		binderBindUnbindLatency();

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
	public void testTargetDestinationWithNull() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
		DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());
		Binding<MessageChannel> consumerBinding = binder.bindConsumer(
				destination0, "testTargetDestinationWithNull", moduleInputChannel, createConsumerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, null)
				.build();

		binderBindUnbindLatency();

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
	public void testTargetDestinationWithNonString() throws Exception {
		SolaceTestBinder binder = getBinder();

		DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

		String destination0 = String.format("foo%s0", getDestinationNameDelimiter());

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, createProducerProperties());

		Message<?> message = MessageBuilder.withPayload("foo".getBytes())
				.setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
				.setHeader(BinderHeaders.TARGET_DESTINATION, 1)
				.build();

		binderBindUnbindLatency();

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
