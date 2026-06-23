package com.solace.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.solace.it.util.semp.config.BrokerConfiguratorBuilder;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder.BrokerConfigurator;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpn;
import com.solacesystems.jcsmp.JCSMPChannelProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * DATAGO-137655 / SOL-50004: closing a bound flow while the JCSMP session is reconnecting blocks in
 * {@code JCSMPBasicSession.waitUntilSessionReconnectDone} until the thread is interrupted.
 *
 * <p>Reproduction: bind a flow against a session with {@code reconnectRetries = -1}, disable the
 * Message VPN to force a perpetual {@code RECONNECTING} loop, then unbind. The producer close is
 * bounded by {@code JCSMPSessionProducerManager.closeSafely}; the consumer close by the existing
 * {@code JCSMPInboundChannelAdapter.stopAllConsumers()} interrupt. Both unbinds must return within
 * {@link #EXPECTED_CLOSE_COMPLETION}.
 *
 * <p>Requires a broker (Testcontainers / {@link PubSubPlusExtension}); runs SAME_THREAD because it
 * disables a shared, broker-side Message VPN.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
class JCSMPCloseFlowReconnectHangIT {
	private static final Logger logger = LoggerFactory.getLogger(JCSMPCloseFlowReconnectHangIT.class);

	/** Max time an {@code unbind()} may take during reconnect (the producer can close two producers at ~10s each). */
	private static final Duration EXPECTED_CLOSE_COMPLETION = Duration.ofSeconds(30);

	private SempV2Api sempV2Api;
	private SpringCloudStreamContext context;
	private String vpnName;
	private BrokerConfigurator brokerConfig;

	// Per-test artifacts cleaned up in @AfterEach.
	private Binding<MessageChannel> producerBinding;
	private Binding<MessageChannel> consumerBinding;
	private boolean vpnDisabled;

	@BeforeEach
	void setUp(JCSMPProperties jcsmpProperties, SempV2Api sempV2Api, SpringCloudStreamContext context) {
		this.sempV2Api = sempV2Api;
		this.context = context;
		this.vpnName = (String) jcsmpProperties.getProperty(JCSMPProperties.VPN_NAME);
		this.brokerConfig = BrokerConfiguratorBuilder.create(sempV2Api).build();

		// Unlimited reconnect-retries is the SOL-50004 precondition: the session must stay in
		// RECONNECTING (never transition to DOWN) while the VPN is disabled, otherwise close()
		// would eventually return on its own and the hang would not reproduce.
		JCSMPChannelProperties channelProperties = (JCSMPChannelProperties) jcsmpProperties
				.getProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES);
		if (channelProperties == null) {
			channelProperties = new JCSMPChannelProperties();
		}
		channelProperties.setReconnectRetries(-1);
		channelProperties.setReconnectRetryWaitInMillis(2000);
		channelProperties.setConnectRetries(1);
		jcsmpProperties.setProperty(JCSMPProperties.CLIENT_CHANNEL_PROPERTIES, channelProperties);
		context.setJcsmpProperties(jcsmpProperties);
	}

	@AfterEach
	void tearDown() {
		// Re-enable the VPN first so any still-blocked close can drain and resources can be freed.
		if (vpnDisabled) {
			try {
				brokerConfig.vpns().updateVpn(new ConfigMsgVpn().msgVpnName(vpnName).enabled(true));
			} catch (Exception e) {
				logger.warn("Failed to re-enable VPN '{}' during cleanup", vpnName, e);
			}
		}
		if (producerBinding != null) {
			try {
				producerBinding.unbind();
			} catch (Exception e) {
				logger.warn("Failed to unbind producer binding during cleanup", e);
			}
		}
		if (consumerBinding != null) {
			try {
				consumerBinding.unbind();
			} catch (Exception e) {
				logger.warn("Failed to unbind consumer binding during cleanup", e);
			}
		}
	}

	/** Producer (DATAGO-137655): unbinding while the session is reconnecting must not hang. */
	@Test
	@Timeout(value = 90, unit = TimeUnit.SECONDS)
	void testProducerStopDoesNotHangWhileSessionReconnecting(TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties =
				context.createProducerProperties(testInfo);
		producerProperties.getExtension().setDestinationType(DestinationType.TOPIC);
		BindingProperties bindingProperties = new BindingProperties();
		bindingProperties.setProducer(producerProperties);

		DirectChannel outputChannel = context.createBindableChannel("output", bindingProperties);
		String destination = RandomStringUtils.randomAlphanumeric(10);
		producerBinding = binder.bindProducer(destination, outputChannel, producerProperties);

		// Confirm the producer is healthy (forces creation of the underlying JCSMP producer).
		outputChannel.send(MessageBuilder.withPayload("before-disruption").build());

		disableVpnAndAwaitReconnecting();

		Binding<MessageChannel> bindingToStop = producerBinding;
		assertClosesWithinTimeout("producer binding unbind()", bindingToStop::unbind);
		producerBinding = null; // already unbound
	}

	/** Message-driven consumer: unbinding while the session is reconnecting must not hang. */
	@Test
	@Timeout(value = 90, unit = TimeUnit.SECONDS)
	void testConsumerStopDoesNotHangWhileSessionReconnecting() throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties =
				context.createConsumerProperties();

		DirectChannel inputChannel = context.createBindableChannel("input", new BindingProperties(), true);
		String destination = RandomStringUtils.randomAlphanumeric(10);
		String group = RandomStringUtils.randomAlphanumeric(10);
		// Binding a consumer provisions the queue and starts an active flow.
		consumerBinding = binder.bindConsumer(destination, group, inputChannel, consumerProperties);

		disableVpnAndAwaitReconnecting();

		Binding<MessageChannel> bindingToStop = consumerBinding;
		assertClosesWithinTimeout("consumer binding unbind()", bindingToStop::unbind);
		consumerBinding = null; // already unbound
	}

	/**
	 * Disables the Message VPN and waits until the broker has dropped its clients, so the binder's
	 * session is reconnecting when the subsequent close is issued.
	 */
	private void disableVpnAndAwaitReconnecting() {
		logger.info("Disabling VPN '{}' to force the session into perpetual RECONNECTING", vpnName);
		brokerConfig.vpns().updateVpn(new ConfigMsgVpn().msgVpnName(vpnName).enabled(false));
		vpnDisabled = true;

		// Wait until the broker reports the VPN disabled; disabling drops its clients, so the binder's
		// session has then lost its connection and entered the (perpetual) reconnect loop.
		Awaitility.await("VPN '" + vpnName + "' reported disabled by broker")
				.atMost(Duration.ofSeconds(30))
				.pollInterval(Duration.ofMillis(250))
				.ignoreExceptions()
				.until(() -> Boolean.FALSE.equals(
						sempV2Api.monitor().getMsgVpn(vpnName, null).getData().isEnabled()));
	}

	/** Runs {@code close} on a daemon thread and asserts it returns within {@link #EXPECTED_CLOSE_COMPLETION} (pre-fix it hangs). */
	private void assertClosesWithinTimeout(String description, Runnable close) {
		ExecutorService executor = Executors.newSingleThreadExecutor(runnable -> {
			Thread thread = new Thread(runnable, "reconnect-hang-close-invoker");
			thread.setDaemon(true);
			return thread;
		});
		try {
			Future<?> future = executor.submit(close);
			assertThatCode(() -> future.get(EXPECTED_CLOSE_COMPLETION.toMillis(), TimeUnit.MILLISECONDS))
					.as("%s must complete within %s while the session is reconnecting; pre-fix it hangs "
							+ "forever in JCSMPBasicSession.waitUntilSessionReconnectDone", description,
							EXPECTED_CLOSE_COMPLETION)
					.doesNotThrowAnyException();
		} finally {
			executor.shutdownNow();
		}
	}
}
