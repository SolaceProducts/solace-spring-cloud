package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.monitor.model.*;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.acks.AckUtils;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * All tests regarding client acknowledgment
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
public class SolaceBinderClientAckIT<T> {
    private static final Logger logger = LoggerFactory.getLogger(SolaceBinderClientAckIT.class);

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, endpointType={2}")
    public void testAccept(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                           @Values(booleans = {false, true}) boolean batchMode,
                           @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
                           SempV2Api sempV2Api,
                           SpringCloudStreamContext context,
                           TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setEndpointType(endpointType);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
                destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                msg -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    AckUtils.accept(ackCallback);
                });

        validateNumEnqueuedMessages(context, sempV2Api, binder.getConsumerQueueName(consumerBinding), endpointType, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, endpointType={2}")
    public void testReject(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                           @Values(booleans = {false, true}) boolean batchMode,
                           @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
                           SempV2Api sempV2Api,
                           SpringCloudStreamContext context,
                           TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setEndpointType(endpointType);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        int numberOfMessages = (batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1) * 3;
        List<Message<?>> messages = IntStream.range(0, numberOfMessages)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String endpointName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 3,
                () -> messages.forEach(moduleOutputChannel::send),
                msg -> {
                    if (isRedelivered(msg, batchMode)) {
                        wasRedelivered.set(true);
                    } else {
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        AckUtils.reject(ackCallback);
                    }
                });

        //rejected message should not be redelivered
        assertThat(wasRedelivered.get()).isFalse();

        validateNumEnqueuedMessages(context, sempV2Api, endpointName, endpointType, 0);
        validateNumRedeliveredMessages(context, sempV2Api, endpointName, endpointType, 0);
        //validateNumAckedMessages(context, sempV2Api, queueName, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, endpointType={2}")
    public void testRejectWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
            JCSMPSession jcsmpSession,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        consumerProperties.getExtension().setEndpointType(endpointType);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
                destination0, group0, moduleInputChannel, consumerProperties);
        List<Message<?>> messages = IntStream.range(0, batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String endpointName = binder.getConsumerQueueName(consumerBinding);
        Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                msg -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    AckUtils.reject(ackCallback);
                });

        assertThat(errorQueue.getName()).satisfies(errorQueueHasMessages(jcsmpSession, messages));

        validateNumEnqueuedMessages(context, sempV2Api, endpointName, endpointType, 0);
        validateNumAckedMessages(context, sempV2Api, endpointName, endpointType, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testRequeue(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                            @Values(booleans = {false, true}) boolean batchMode,
                            @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
                            SempV2Api sempV2Api,
                            SpringCloudStreamContext context,
                            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setEndpointType(endpointType);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String endpointName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 2,
                () -> messages.forEach(moduleOutputChannel::send),
                msg -> {
                    if (isRedelivered(msg, batchMode)) {
                        wasRedelivered.set(true);
                    } else {
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        AckUtils.requeue(ackCallback);
                    }
                });
        assertThat(wasRedelivered.get()).isTrue();

        validateNumEnqueuedMessages(context, sempV2Api, endpointName, endpointType, 0);
        validateNumRedeliveredMessages(context, sempV2Api, endpointName, endpointType, messages.size());
        validateNumAckedMessages(context, sempV2Api, endpointName, endpointType, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAsyncAccept(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                                @Values(booleans = {false, true}) boolean batchMode,
                                SempV2Api sempV2Api,
                                SpringCloudStreamContext context,
                                @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
                                SoftAssertions softly,
                                TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    logger.info("Received message");
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    executorService.schedule(() -> {
                        softly.assertThat(queueName)
                                .satisfies(q -> validateNumEnqueuedMessages(context, sempV2Api, q, messages.size()));
                        logger.info("Async acknowledging message");
                        AckUtils.accept(ackCallback);
                        callback.run();
                    }, 2, TimeUnit.SECONDS);
                });

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAsyncReject(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                                @Values(booleans = {false, true}) boolean batchMode,
                                SempV2Api sempV2Api,
                                SpringCloudStreamContext context,
                                SoftAssertions softly,
                                @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
                                TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        wasRedelivered.set(true);
                        callback.run();
                    } else {
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        executorService.schedule(() -> {
                            softly.assertThat(queueName)
                                    .satisfies(q -> validateNumEnqueuedMessages(context, sempV2Api, q, messages.size()));
                            AckUtils.reject(ackCallback);
                            callback.run();
                        }, 2, TimeUnit.SECONDS);
                    }
                });

        softly.assertAll();
        assertThat(wasRedelivered.get()).isFalse();
        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAsyncRejectWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            JCSMPSession jcsmpSession,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
            SoftAssertions softly,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
                moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String queueName = binder.getConsumerQueueName(consumerBinding);
        Queue errorQueue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerErrorQueueName(consumerBinding));

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    executorService.schedule(() -> {
                        softly.assertThat(queueName)
                                .satisfies(q -> validateNumEnqueuedMessages(context, sempV2Api, q, messages.size()));
                        AckUtils.reject(ackCallback);
                        callback.run();
                    }, 2, TimeUnit.SECONDS);
                });

        assertThat(errorQueue.getName()).satisfies(errorQueueHasMessages(jcsmpSession, messages));
        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAsyncRequeue(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                                 @Values(booleans = {false, true}) boolean batchMode,
                                 SempV2Api sempV2Api,
                                 SpringCloudStreamContext context,
                                 @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
                                 SoftAssertions softly,
                                 TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 2,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        wasRedelivered.set(true);
                        callback.run();
                    } else {
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        executorService.schedule(() -> {
                            softly.assertThat(queueName)
                                    .satisfies(q -> validateNumEnqueuedMessages(context, sempV2Api, q, messages.size()));
                            AckUtils.requeue(ackCallback);
                            callback.run();
                        }, 2, TimeUnit.SECONDS);
                    }
                });
        assertThat(wasRedelivered.get()).isTrue();

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, endpointType={2}")
    public void testNoAck(@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
                          @Values(booleans = {false, true}) boolean batchMode,
                          @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
                          SempV2Api sempV2Api,
                          SpringCloudStreamContext context,
                          TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setEndpointType(endpointType);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);


        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String endpointName = binder.getConsumerQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 1,
                () -> messages.forEach(moduleOutputChannel::send),
                msg -> Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg)).noAutoAck());

        // Give some time just to make sure
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));

        validateNumEnqueuedMessages(context, sempV2Api, endpointName, endpointType, messages.size());
        validateNumRedeliveredMessages(context, sempV2Api, endpointName, endpointType, 0);
        validateNumAckedMessages(context, sempV2Api, endpointName, endpointType, 0);
        validateNumUnackedMessages(context, sempV2Api, endpointName, endpointType, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testNoAckAndThrowException(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts() + 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        callback.run();
                    } else {
                        logger.info("Received message");
                        Objects.requireNonNull(StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg)).noAutoAck();
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, messages.size());
        validateNumAckedMessages(context, sempV2Api, queueName, messages.size());
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAcceptAndThrowException(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        final CountDownLatch redeliveredLatch = new CountDownLatch(1);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(acknowledgmentCallback).noAutoAck();
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        redeliveredLatch.countDown();
                    } else {
                        logger.info("Receiving message");
                        AckUtils.accept(acknowledgmentCallback);
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });
        assertThat(redeliveredLatch.await(3, TimeUnit.SECONDS)).isFalse();

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
        validateNumAckedMessages(context, sempV2Api, queueName, messages.size());
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testRejectAndThrowException(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        wasRedelivered.set(true);
                        callback.run();
                    } else {
                        logger.info("Receiving message");
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        AckUtils.reject(ackCallback);
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });

        assertThat(wasRedelivered.get()).isFalse();
        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
        validateNumAckedMessages(context, sempV2Api, queueName, 0);
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testRequeueAndThrowException(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts() + 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        callback.run();
                    } else {
                        logger.info("Receiving message");
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        AckUtils.requeue(ackCallback);
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, messages.size());
        validateNumAckedMessages(context, sempV2Api, queueName, messages.size());
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}, endpointType={2}")
    public void testNoAckAndThrowExceptionWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
            JCSMPSession jcsmpSession,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setQueueMaxMsgRedelivery(5);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                group0, moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String endpointName = binder.getConsumerQueueName(consumerBinding);
        String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    callback.run();
                    throw new RuntimeException("expected exception");
                });

        assertThat(errorQueueName).satisfies(errorQueueHasMessages(jcsmpSession, messages));
        validateNumEnqueuedMessages(context, sempV2Api, endpointName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, endpointName, 0);
        validateNumUnackedMessages(context, sempV2Api, endpointName, 0);
        validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testAcceptAndThrowExceptionWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                group0, moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String queueName = binder.getConsumerQueueName(consumerBinding);
        String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

        final CountDownLatch redeliveredLatch = new CountDownLatch(1);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback acknowledgmentCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(acknowledgmentCallback).noAutoAck();
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        redeliveredLatch.countDown();
                    } else {
                        logger.info("Receiving message");
                        AckUtils.accept(acknowledgmentCallback);
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });

        assertThat(redeliveredLatch.await(3, TimeUnit.SECONDS)).isFalse();

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
        validateNumAckedMessages(context, sempV2Api, queueName, messages.size());
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);
        validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testRejectAndThrowExceptionWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            JCSMPSession jcsmpSession,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
                moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String queueName = binder.getConsumerQueueName(consumerBinding);
        String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    AckUtils.reject(ackCallback);
                    callback.run();
                    throw new RuntimeException("expected exception");
                });

        assertThat(errorQueueName).satisfies(errorQueueHasMessages(jcsmpSession, messages));
        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, 0);
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);
        validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
    public void testRequeueAndThrowExceptionWithErrorQueue(
            @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
            @Values(booleans = {false, true}) boolean batchMode,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);
        String group0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(batchMode);
        consumerProperties.getExtension().setAutoBindErrorQueue(true);
        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0, group0,
                moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0,
                        batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1)
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();

        String queueName = binder.getConsumerQueueName(consumerBinding);
        String errorQueueName = binder.getConsumerErrorQueueName(consumerBinding);

        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts() + 1,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    if (isRedelivered(msg, batchMode)) {
                        logger.info("Received redelivered message");
                        callback.run();
                    } else {
                        logger.info("Receiving message");
                        AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                        Objects.requireNonNull(ackCallback).noAutoAck();
                        AckUtils.requeue(ackCallback);
                        callback.run();
                        throw new RuntimeException("expected exception");
                    }
                });

        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, messages.size());
        validateNumAckedMessages(context, sempV2Api, queueName, messages.size());
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);
        validateNumEnqueuedMessages(context, sempV2Api, errorQueueName, 0);

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    @ParameterizedTest(name = "[{index}] channelType={0}")
    @ValueSource(classes = {DirectChannel.class, PollableSource.class})
    public void testBatchIsNotStaleFromAsyncRequeue(
            Class<T> channelType,
            SempV2Api sempV2Api,
            SpringCloudStreamContext context,
            @ExecSvc(poolSize = 1, scheduled = true) ScheduledExecutorService executorService,
            SoftAssertions softly,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();
        ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
        T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

        String destination0 = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> producerBinding = binder.bindProducer(
                destination0, moduleOutputChannel, context.createProducerProperties(testInfo));

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        consumerProperties.setBatchMode(true);
        consumerProperties.getExtension().setBatchMaxSize(2);

        Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder, destination0,
                RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

        List<Message<?>> messages = IntStream.range(0, consumerProperties.getExtension().getBatchMaxSize())
                .mapToObj(i -> MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                        .build())
                .collect(Collectors.toList());

        context.binderBindUnbindLatency();
        String queueName = binder.getConsumerQueueName(consumerBinding);

        AtomicBoolean firstReceivedMessage = new AtomicBoolean(false);
        AtomicBoolean wasRedelivered = new AtomicBoolean(false);
        consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, 2,
                () -> messages.forEach(moduleOutputChannel::send),
                (msg, callback) -> {
                    AcknowledgmentCallback ackCallback = StaticMessageHeaderAccessor.getAcknowledgmentCallback(msg);
                    Objects.requireNonNull(ackCallback).noAutoAck();
                    softly.assertThat(msg).as("first batch is not valid")
                            .satisfies(isValidMessage(consumerProperties,
                                    messages.subList(0, consumerProperties.getExtension().getBatchMaxSize())));
                    if (!firstReceivedMessage.get()) {
                        logger.info("Got first message");
                        executorService.schedule(() -> {
                            softly.assertThat(queueName).satisfies(q ->
                                    validateNumEnqueuedMessages(context, sempV2Api, q, messages.size()));
                            AckUtils.requeue(ackCallback);
                            callback.run();
                        }, 2, TimeUnit.SECONDS);
                        firstReceivedMessage.set(true);
                    } else if (isRedelivered(msg, true)) {
                        logger.info("Got redelivered message");
                        wasRedelivered.set(true);
                        try {
                            ackCallback.acknowledge(AcknowledgmentCallback.Status.ACCEPT);
                        } catch (Exception e) {
                            softly.fail(String.format(
                                    "Exception caught when trying to process redelivered batch %s", msg), e);
                            throw e;
                        }
                        callback.run();
                    } else {
                        softly.fail("Found a message that is not marked redelivered: %s", msg);
                    }
                });
        assertThat(wasRedelivered.get()).isTrue();

        // No leftover message stuck in batch collector
        validateNumEnqueuedMessages(context, sempV2Api, queueName, 0);
        validateNumUnackedMessages(context, sempV2Api, queueName, 0);
        validateNumRedeliveredMessages(context, sempV2Api, queueName, messages.size());

        producerBinding.unbind();
        consumerBinding.unbind();
    }

    private boolean isRedelivered(Message<?> message, boolean batchMode) {
        SoftAssertions isRedelivered = new SoftAssertions();
        isRedelivered.assertThat(message).satisfies(hasNestedHeader(SolaceHeaders.REDELIVERED, Boolean.class,
                batchMode, v -> assertThat(v).isNotNull().isTrue()));
        return isRedelivered.wasSuccess();
    }

    private void validateNumEnqueuedMessages(SpringCloudStreamContext context,
                                             SempV2Api sempV2Api,
                                             String endpointName,
                                             EndpointType endpointType,
                                             int expectedCount) throws InterruptedException {
        String vpnName = (String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
        retryAssert(1, TimeUnit.MINUTES, () -> {
            List<Object> messages = new ArrayList<>();
            Optional<String> cursor = Optional.empty();
            do {
                switch (endpointType) {
                    case QUEUE:
                        MonitorMsgVpnQueueMsgsResponse responseQ = sempV2Api.monitor()
                                .getMsgVpnQueueMsgs(vpnName, endpointName, Integer.MAX_VALUE, cursor.orElse(null),
                                        null, null);
                        cursor = Optional.ofNullable(responseQ.getMeta())
                                .map(MonitorSempMeta::getPaging)
                                .map(MonitorSempPaging::getCursorQuery);
                        messages.addAll(responseQ.getData());
                        break;
                    case TOPIC_ENDPOINT:
                        MonitorMsgVpnTopicEndpointMsgsResponse responseT = sempV2Api.monitor()
                                .getMsgVpnTopicEndpointMsgs(vpnName, endpointName, Integer.MAX_VALUE, cursor.orElse(null),
                                        null, null);
                        cursor = Optional.ofNullable(responseT.getMeta())
                                .map(MonitorSempMeta::getPaging)
                                .map(MonitorSempPaging::getCursorQuery);
                        messages.addAll(responseT.getData());
                        break;
                }
            } while (cursor.isPresent());
            assertThat(messages)
                    .as("Unexpected number of messages on endpoint %s", endpointName)
                    .hasSize(expectedCount);
        });
    }

    private void validateNumEnqueuedMessages(SpringCloudStreamContext context,
                                             SempV2Api sempV2Api,
                                             String queueName,
                                             int expectedCount) throws InterruptedException {
        validateNumEnqueuedMessages(context, sempV2Api, queueName, EndpointType.QUEUE,
                expectedCount);
    }

    private void validateNumAckedMessages(SpringCloudStreamContext context,
                                          SempV2Api sempV2Api,
                                          String queueName,
                                          int expectedCount) throws InterruptedException {
        validateNumAckedMessages(context, sempV2Api, queueName, EndpointType.QUEUE, expectedCount);
    }

    private void validateNumAckedMessages(SpringCloudStreamContext context,
                                          SempV2Api sempV2Api,
                                          String endpointName,
                                          EndpointType endpointType,
                                          int expectedCount) throws InterruptedException {
        switch (endpointType) {
            case QUEUE -> retryAssert(
                    () -> assertThat(
                            sempV2Api.monitor()
                                    .getMsgVpnQueueTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                                            endpointName, 2, null, null, null)
                                    .getData())
                            .as("Unexpected number of flows on queue %s", endpointName)
                            .hasSize(1)
                            .allSatisfy(txFlow -> assertThat(txFlow.getAckedMsgCount())
                                    .as("Unexpected number of acked messages on flow %s of queue %s",
                                            txFlow.getFlowId(), endpointName)
                                    .isEqualTo(expectedCount)));

            case TOPIC_ENDPOINT -> retryAssert(
                    () -> assertThat(
                            sempV2Api.monitor()
                                    .getMsgVpnTopicEndpointTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                                            endpointName, 2, null, null, null)
                                    .getData())
                            .as("Unexpected number of flows on queue %s", endpointName)
                            .hasSize(1)
                            .allSatisfy(txFlow -> assertThat(txFlow.getAckedMsgCount())
                                    .as("Unexpected number of acked messages on flow %s of queue %s",
                                            txFlow.getFlowId(), endpointName)
                                    .isEqualTo(expectedCount)));
        }
    }

    private void validateNumUnackedMessages(SpringCloudStreamContext context,
                                            SempV2Api sempV2Api,
                                            String queueName,
                                            int expectedCount) throws InterruptedException {
        validateNumUnackedMessages(context, sempV2Api, queueName, EndpointType.QUEUE, expectedCount);
    }

    private void validateNumUnackedMessages(SpringCloudStreamContext context,
                                            SempV2Api sempV2Api,
                                            String endpointName,
                                            EndpointType endpointType,
                                            int expectedCount) throws InterruptedException {
        switch (endpointType) {
            case QUEUE -> retryAssert(() -> assertThat(sempV2Api.monitor()
                    .getMsgVpnQueueTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                            endpointName, 2, null, null, null)
                    .getData())
                    .as("Unexpected number of flows on queue %s", endpointName)
                    .hasSize(1)
                    .allSatisfy(txFlow -> assertThat(txFlow.getUnackedMsgCount())
                            .as("Unexpected number of unacked messages on flow %s of queue %s",
                                    txFlow.getFlowId(), endpointName)
                            .isEqualTo(expectedCount)));
            case TOPIC_ENDPOINT -> retryAssert(() -> assertThat(sempV2Api.monitor()
                    .getMsgVpnTopicEndpointTxFlows((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                            endpointName, 2, null, null, null)
                    .getData())
                    .as("Unexpected number of flows on topic endpoint %s", endpointName)
                    .hasSize(1)
                    .allSatisfy(txFlow -> assertThat(txFlow.getUnackedMsgCount())
                            .as("Unexpected number of unacked messages on flow %s of topic endpoint %s",
                                    txFlow.getFlowId(), endpointName)
                            .isEqualTo(expectedCount)));
        }

    }

    private void validateNumRedeliveredMessages(SpringCloudStreamContext context,
                                                SempV2Api sempV2Api,
                                                String queueName,
                                                int expectedCount) throws InterruptedException {
        validateNumRedeliveredMessages(context, sempV2Api, queueName, EndpointType.QUEUE, expectedCount);
    }

    private void validateNumRedeliveredMessages(SpringCloudStreamContext context,
                                                SempV2Api sempV2Api,
                                                String endpointName,
                                                EndpointType endpointType,
                                                int expectedCount) throws InterruptedException {
        retryAssert(() -> assertThat(
                switch (endpointType) {
                    case QUEUE -> sempV2Api.monitor()
                            .getMsgVpnQueue((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                                    endpointName, null)
                            .getData()
                            .getRedeliveredMsgCount();
                    case TOPIC_ENDPOINT -> sempV2Api.monitor().getMsgVpnTopicEndpoint((String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME),
                            endpointName, null).getData().getRedeliveredMsgCount();
                }
        ).as("Unexpected number of redeliveries on endpoint %s", endpointName)
                .isEqualTo(expectedCount));
    }
}
