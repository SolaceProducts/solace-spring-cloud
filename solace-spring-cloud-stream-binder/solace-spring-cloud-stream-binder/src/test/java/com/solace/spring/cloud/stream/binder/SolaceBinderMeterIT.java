package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.config.SolaceMeterConfiguration;
import com.solace.spring.cloud.stream.binder.meter.SolaceMessageMeterBinder;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.MessageGenerator;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.spring.configuration.TestMeterRegistryConfiguration;
import com.solace.spring.cloud.stream.binder.test.util.SimpleJCSMPEventHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueueSubscription;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.FlowReceiver;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeTypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions.isValidMessageSizeMeter;
import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(classes = {
		TestMeterRegistryConfiguration.class,
		SolaceJavaAutoConfiguration.class,
		SolaceMeterConfiguration.class},
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderMeterIT {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderMeterIT.class);

	@BeforeAll
	static void beforeAll(@Autowired SolaceMessageMeterBinder messageMeterBinder,
						  @Autowired MeterRegistry meterRegistry) {
		messageMeterBinder.bindTo(meterRegistry);
	}

	@BeforeEach
	void setUp(@Autowired SolaceMeterAccessor solaceMeterAccessor,
			   SpringCloudStreamContext context) {
		context.getBinder().getBinder().setSolaceMeterAccessor(solaceMeterAccessor);
	}

	@CartesianTest(name = "[{index}] channelType={0}, batchMode={1}")
	public <T> void testConsumerMeters(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
			@Values(booleans = {false, true}) boolean batchMode,
			@Autowired SimpleMeterRegistry meterRegistry,
			JCSMPSession jcsmpSession,
			SpringCloudStreamContext context,
			@ExecSvc(scheduled = true, poolSize = 1) ScheduledExecutorService executorService) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		T moduleInputChannel = consumerInfrastructureUtil.createChannel(
				RandomStringUtils.randomAlphanumeric(100),
				new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.setBatchMode(batchMode);
		consumerProperties.getExtension().setEndpointType(endpointType);
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		int numMessages = batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1;
		List<XMLMessage> messages = IntStream.range(0, numMessages)
				.mapToObj(i -> JCSMPFactory.onlyInstance().createMessage(BytesMessage.class))
				.peek(m -> {
					byte[] data = UUID.randomUUID().toString().getBytes();
					m.setData(data);
					assertThat(m)
							.extracting(XMLMessage::getAttachmentContentLength)
							.as("Message has an attachment length")
							.isEqualTo(data.length);
				})
				.peek(m -> {
					byte[] bytes = UUID.randomUUID().toString().getBytes();
					m.writeBytes(bytes);
					assertThat(m)
							.extracting(XMLMessage::getContentLength)
							.as("Message has a content length")
							.isEqualTo(bytes.length);
				})
				.peek(m -> {
					m.setProperties(JCSMPFactory.onlyInstance().createMap());
					try {
						m.getProperties().putString(UUID.randomUUID().toString(), UUID.randomUUID().toString());
					} catch (SDTException e) {
						throw new RuntimeException(e);
					}
				})
				.collect(Collectors.toList());

		context.binderBindUnbindLatency();
		consumerProperties.populateBindingName(consumerBinding.getBindingName());

		consumerInfrastructureUtil.subscribe(moduleInputChannel, executorService, msg -> {});

		int defaultBinaryMetadataContentLength;
		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new SimpleJCSMPEventHandler());
		try {
			for (XMLMessage message : messages) {
				producer.send(message, JCSMPFactory.onlyInstance().createTopic(destination0));
			}
			BytesMessage defaultMessage = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
			producer.send(defaultMessage,
					JCSMPFactory.onlyInstance().createTopic(RandomStringUtils.randomAlphanumeric(100)));
			defaultBinaryMetadataContentLength = defaultMessage.getBinaryMetadataContentLength(0);
		} finally {
			producer.close();
		}

		assertThat(messages)
				.extracting(m -> m.getBinaryMetadataContentLength(0))
				.as("Message has binary metadata content length")
				.allSatisfy(length -> assertThat(length).isGreaterThan(defaultBinaryMetadataContentLength));

		logger.info("Validating message size meters");
		retryAssert(() -> {
			assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_PAYLOAD_SIZE)
					.tag(SolaceMessageMeterBinder.TAG_NAME, consumerProperties.getBindingName())
					.meters())
					.hasSize(1)
					.first()
					.as("Checking meter %s with name %s",
							SolaceMessageMeterBinder.METER_NAME_PAYLOAD_SIZE, consumerProperties.getBindingName())
					.satisfies(isValidMessageSizeMeter(consumerProperties.getBindingName(), numMessages,
							messages.stream()
									.map(m -> m.getContentLength() + m.getAttachmentContentLength())
									.mapToLong(l -> l)
									.sum()));

			assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_TOTAL_SIZE)
					.tag(SolaceMessageMeterBinder.TAG_NAME, consumerProperties.getBindingName())
					.meters())
					.hasSize(1)
					.first()
					.as("Checking meter %s with name %s",
							SolaceMessageMeterBinder.METER_NAME_TOTAL_SIZE, consumerProperties.getBindingName())
					.satisfies(isValidMessageSizeMeter(consumerProperties.getBindingName(), numMessages,
							messages.stream()
									.map(m -> m.getContentLength() + m.getAttachmentContentLength() +
											m.getBinaryMetadataContentLength(0))
									.mapToLong(l -> l)
									.sum()));
		});

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] batched={0}")
	public void testProducerMeters(@Values(booleans = {false, true}) boolean batched,
								   @Autowired SimpleMeterRegistry meterRegistry,
								   JCSMPSession jcsmpSession,
								   SpringCloudStreamContext context,
								   Queue queue,
								   SempV2Api sempV2Api,
								   TestInfo testInfo) throws Exception {
		String destination0 = RandomStringUtils.randomAlphanumeric(10);
		sempV2Api.config().createMsgVpnQueueSubscription(
				new ConfigMsgVpnQueueSubscription().subscriptionTopic(destination0),
				(String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME),
				queue.getName(),
				null, null);

		SolaceTestBinder binder = context.getBinder();

		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context
				.createProducerProperties(testInfo);
		producerProperties.setUseNativeEncoding(true);

		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);

		DirectChannel moduleOutputChannel = context.createBindableChannel(
				RandomStringUtils.randomAlphanumeric(100), bindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, producerProperties);

		context.binderBindUnbindLatency();
		producerProperties.populateBindingName(producerBinding.getBindingName());

		MessageGenerator.BatchingConfig batchingConfig = new MessageGenerator.BatchingConfig()
				.setEnabled(batched);
		Message<?> message = MessageGenerator.generateMessage(
				i -> RandomStringUtils.randomAlphanumeric(100),
				i -> Map.of(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE),
				batchingConfig)
				.build();
		int numMessages = batched ? batchingConfig.getNumberOfMessages() : 1;

		moduleOutputChannel.send(message);

		FlowReceiver flowReceiver = jcsmpSession.createFlow(null,
				new ConsumerFlowProperties().setEndpoint(queue).setStartState(true));
		List<BytesXMLMessage> receivedMessages = new ArrayList<>();
		try {
			logger.info("Consuming messages");
			for (int i = 0; i < numMessages; i++) {
				BytesXMLMessage receivedMessage = flowReceiver.receive((int) TimeUnit.MINUTES.toMillis(1));
				assertThat(receivedMessage).as("Didn't receive message within timeout").isNotNull();
				receivedMessages.add(receivedMessage);
			}
		} finally {
			flowReceiver.close();
		}

		assertThat(receivedMessages).hasSize(numMessages);

		int aggregateContentLength = receivedMessages.stream()
				.mapToInt(XMLMessage::getContentLength)
				.sum();
		int aggregateAttachmentContentLength = receivedMessages.stream()
				.mapToInt(XMLMessage::getAttachmentContentLength)
				.sum();
		int aggregateBinaryMetadataContentLength = receivedMessages.stream()
				.mapToInt(m -> m.getBinaryMetadataContentLength(0))
				.sum();


		logger.info("Validating message size meters");
		retryAssert(() -> {
			assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_PAYLOAD_SIZE)
					.tag(SolaceMessageMeterBinder.TAG_NAME, producerProperties.getBindingName())
					.meters())
					.singleElement()
					.as("Checking meter %s with name %s",
							SolaceMessageMeterBinder.METER_NAME_PAYLOAD_SIZE, producerProperties.getBindingName())
					.satisfies(isValidMessageSizeMeter(producerProperties.getBindingName(), numMessages,
							aggregateContentLength + aggregateAttachmentContentLength));

			assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_TOTAL_SIZE)
					.tag(SolaceMessageMeterBinder.TAG_NAME, producerProperties.getBindingName())
					.meters())
					.singleElement()
					.as("Checking meter %s with name %s",
							SolaceMessageMeterBinder.METER_NAME_TOTAL_SIZE, producerProperties.getBindingName())
					.satisfies(isValidMessageSizeMeter(producerProperties.getBindingName(), numMessages,
							aggregateContentLength +
									aggregateAttachmentContentLength +
									aggregateBinaryMetadataContentLength));
		});

		producerBinding.unbind();
	}
}
