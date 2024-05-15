package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.config.SolaceHealthIndicatorsConfiguration;
import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.health.contributors.BindingHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.BindingsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.FlowsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.SolaceBinderHealthContributor;
import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceSpringCloudStreamAssertions;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.NamedContributor;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(classes = {
		SolaceHealthIndicatorsConfiguration.class,
		SolaceJavaAutoConfiguration.class
}, initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderHealthIT {
	private static final Logger logger = LoggerFactory.getLogger(SolaceBinderHealthIT.class);

	@CartesianTest(name = "[{index}] channelType={0}, autoStart={1} concurrency={2}")
	public <T> void testConsumerFlowHealthProvisioning(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(booleans = {true, false}) boolean autoStart,
			@Values(ints = {1, 3}) int concurrency,
			SpringCloudStreamContext context) throws Exception {
		if (concurrency > 1 && channelType.equals(PollableSource.class)) {
			return;
		}

		SolaceTestBinder binder = context.getBinder();

		BindingsHealthContributor bindingsHealthContributor = new BindingsHealthContributor();
		binder.getBinder().setSolaceBinderHealthAccessor(new SolaceBinderHealthAccessor(
				new SolaceBinderHealthContributor(new SessionHealthIndicator(new SolaceSessionHealthProperties()),
						bindingsHealthContributor)));

		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(10));
		consumerProperties.setAutoStartup(autoStart);
		consumerProperties.setConcurrency(concurrency);

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		context.binderBindUnbindLatency();

		if (!autoStart) {
			assertThat(bindingsHealthContributor.iterator().hasNext()).isFalse();
			logger.info("Starting binding...");
			consumerBinding.start();
		}

		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), concurrency, Status.UP));

		logger.info("Pausing binding...");
		consumerBinding.pause();
		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), concurrency, Status.UP));

		logger.info("Stopping binding...");
		consumerBinding.stop();
		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.extracting(c -> c.getContributor(consumerProperties.getBindingName()))
				.extracting(BindingHealthContributor::getFlowsHealthContributor)
				.extracting(f -> StreamSupport.stream(f.spliterator(), false))
				.asInstanceOf(InstanceOfAssertFactories.stream(NamedContributor.class))
				.isEmpty();

		logger.info("Starting binding...");
		consumerBinding.start();
		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), concurrency, Status.UP));

		logger.info("Resuming binding...");
		consumerBinding.resume();
		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), concurrency, Status.UP));

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}, concurrency={1} healthStatus={2}")
	public <T> void testConsumerFlowHealthUnhealthy(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			@Values(ints = {1, 3}) int concurrency,
			@Values(strings = {"DOWN", "RECONNECTING"}) String healthStatus,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context) throws Exception {
		if (concurrency > 1 && channelType.equals(PollableSource.class)) {
			return;
		}

		SolaceTestBinder binder = context.getBinder();

		BindingsHealthContributor bindingsHealthContributor = new BindingsHealthContributor();
		binder.getBinder().setSolaceBinderHealthAccessor(new SolaceBinderHealthAccessor(
				new SolaceBinderHealthContributor(new SessionHealthIndicator(new SolaceSessionHealthProperties()),
						bindingsHealthContributor)));

		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(10));
		consumerProperties.setConcurrency(concurrency);

		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		context.binderBindUnbindLatency();

		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), concurrency, Status.UP));

		String vpnName = (String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
		String queueName = binder.getConsumerQueueName(consumerBinding);
		logger.info(String.format("Disabling egress for queue %s", queueName));
		switch (healthStatus) {
			case "DOWN" -> sempV2Api.config().deleteMsgVpnQueue(vpnName, queueName);
			case "RECONNECTING" -> sempV2Api.config()
					.updateMsgVpnQueue(vpnName, queueName, new ConfigMsgVpnQueue().egressEnabled(false), null);
			default -> throw new IllegalArgumentException("No test for health status: " + healthStatus);
		}

		retryAssert(2, TimeUnit.MINUTES,
				() -> assertThat(bindingsHealthContributor)
						.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
						.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(
								consumerProperties.getBindingName(), concurrency, new Status(healthStatus))));

		if (healthStatus.equals("RECONNECTING")) {
			sempV2Api.config()
					.updateMsgVpnQueue(vpnName, queueName, new ConfigMsgVpnQueue().egressEnabled(true), null);
			retryAssert(2, TimeUnit.MINUTES,
					() -> assertThat(bindingsHealthContributor)
							.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
							.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(
									consumerProperties.getBindingName(), concurrency, Status.UP)));
		}

		consumerBinding.unbind();
	}

	@CartesianTest(name = "[{index}] channelType={0}")
	public <T> void testConsumerFlowHealthNack(
			@Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		BindingsHealthContributor bindingsHealthContributor = Mockito.spy(new BindingsHealthContributor());
		binder.getBinder().setSolaceBinderHealthAccessor(new SolaceBinderHealthAccessor(
				new SolaceBinderHealthContributor(new SessionHealthIndicator(new SolaceSessionHealthProperties()),
						bindingsHealthContributor)));

		ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);

		DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());
		T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());

		String destination0 = RandomStringUtils.randomAlphanumeric(10);

		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
		consumerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(10));

		Binding<MessageChannel> producerBinding = binder.bindProducer(
				destination0, moduleOutputChannel, context.createProducerProperties(testInfo));
		Binding<T> consumerBinding = consumerInfrastructureUtil.createBinding(binder,
				destination0, RandomStringUtils.randomAlphanumeric(10), moduleInputChannel, consumerProperties);

		context.binderBindUnbindLatency();

		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), 1, Status.UP));

		String flowHealthId = "flow-0";
		BindingHealthContributor bindingHealthContributor = (BindingHealthContributor) bindingsHealthContributor
				.getContributor(consumerProperties.getBindingName());
		FlowsHealthContributor flowsHealthContributor = bindingHealthContributor.getFlowsHealthContributor();

		logger.info("Injecting Mockito spy into flow health indicator: {}", flowHealthId);
		FlowHealthIndicator flowHealthIndicator = Mockito.spy((FlowHealthIndicator) (flowsHealthContributor
				.getContributor(flowHealthId)));
		flowsHealthContributor.removeFlowContributor(flowHealthId);
		flowsHealthContributor.addFlowContributor(flowHealthId, flowHealthIndicator);

		logger.info("Injecting Mockito spy into flows health indicator for binding: {}", consumerProperties.getBindingName());
		flowsHealthContributor = Mockito.spy(flowsHealthContributor);
		bindingsHealthContributor.removeBindingContributor(consumerProperties.getBindingName());
		bindingsHealthContributor.addBindingContributor(consumerProperties.getBindingName(),
				Mockito.spy(new BindingHealthContributor(flowsHealthContributor)));

		// Clear invocations due to spy injection
		// Real test begins now...
		Mockito.clearInvocations(bindingsHealthContributor);

		consumerInfrastructureUtil.sendAndSubscribe(moduleInputChannel, consumerProperties.getMaxAttempts(),
				() -> moduleOutputChannel.send(MessageBuilder.withPayload(UUID.randomUUID().toString().getBytes())
						.build()),
				(msg, callback) -> {
					callback.run();
					throw new RuntimeException("Throwing expected exception!");
				});

		Mockito.verify(flowHealthIndicator, Mockito.never()
				.description("Message NACK should not have caused health to go down"))
				.down(Mockito.any());
		Mockito.verify(flowsHealthContributor, Mockito.never()
						.description("Message NACK should not have caused flow health indicator to be removed"))
				.removeFlowContributor(Mockito.any());
		Mockito.verify(bindingsHealthContributor, Mockito.never()
						.description("Message NACK should not have caused health component to be removed"))
				.removeBindingContributor(Mockito.any());

		assertThat(bindingsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingsHealthContributor.class))
				.satisfies(SolaceSpringCloudStreamAssertions.isSingleBindingHealthAvailable(consumerProperties.getBindingName(), 1, Status.UP));

		producerBinding.unbind();
		consumerBinding.unbind();
	}
}
