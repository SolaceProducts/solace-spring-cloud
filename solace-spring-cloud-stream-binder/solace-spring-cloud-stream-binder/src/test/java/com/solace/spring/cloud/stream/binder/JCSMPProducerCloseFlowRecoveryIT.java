package com.solace.spring.cloud.stream.binder;

import com.solace.it.util.semp.config.BrokerConfiguratorBuilder;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder.BrokerConfigurator;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SimpleJCSMPEventHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.StaleSessionException;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.DockerClientFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Broker integration tests characterising producer behaviour around the
 * <a href="https://sol-jira.atlassian.net/browse/DATAGO-134580">DATAGO-134580</a> bug: when the
 * broker tears down a publisher flow (DR failover, message-spool maintenance), JCSMP marks the
 * per-binding {@link XMLMessageProducer} as terminally closed and every subsequent
 * {@code send(...)} throws {@link StaleSessionException} until the app is restarted.
 *
 * <p>This class contains five integration tests. Three of them are <b>control / negative
 * cases</b> that exercise broker-side disruptions which - empirically - do <em>not</em>
 * reproduce the bug. They are kept as long-term coverage so that any future broker behaviour
 * change (e.g. a future broker version starts ejecting already-bound publisher flows on a
 * spool-quota change) shows up immediately as a test failure rather than a silent regression.
 * The fourth test is the <b>bug reproducer</b> that pins the exact customer-reported failure
 * mode and is what the {@code JCSMPOutboundMessageHandler} fix must turn green. The fifth
 * test loops the bug reproducer across multiple consecutive stale-flow cycles, proving the
 * recreate-on-stale path resets cleanly between events and does not accumulate state.
 *
 * <ol>
 *   <li>{@link #test_persistentTopicPublisher_survivesSpoolToggle persistentTopicPublisher_survivesSpoolToggle}
 *       - <b>Control.</b> Persistent (GD) publishing to a topic + VPN-level spool quota
 *       cycled through 0 via SEMPv2. Asserts the publisher continues to publish. The broker
 *       only NACKs new spool writes asynchronously when the quota is 0; it does not eject
 *       already-bound publisher flows.</li>
 *
 *   <li>{@link #test_directTopicPublisher_survivesSpoolToggle directTopicPublisher_survivesSpoolToggle}
 *       - <b>Control.</b> DIRECT (non-persistent) publishing to a topic via raw JCSMP +
 *       the same spool toggle. Asserts the publisher continues to publish. Direct messages
 *       bypass the spool subsystem entirely so the toggle is a no-op for them. The binder
 *       cannot be used for this case because {@code XMLMessageMapper.java:205} unconditionally
 *       promotes outbound messages to {@link DeliveryMode#PERSISTENT}.</li>
 *
 *   <li>{@link #test_persistentQueuePublisher_survivesQueueIngressEgressToggle persistentQueuePublisher_survivesQueueIngressEgressToggle}
 *       - <b>Control.</b> Persistent publishing to a Queue destination + the queue's ingress
 *       and egress disabled and re-enabled via SEMPv2. Asserts the publisher continues to
 *       publish. Toggling ingress/egress causes the broker to NACK incoming publishes
 *       asynchronously while disabled but does <em>not</em> fan out unsolicited
 *       {@code CloseFlow}, so the bound publisher flow stays alive across the cycle.</li>
 *
 *   <li>{@link #test_persistentQueuePublisher_recoversAfterMessageSpoolCliShutdown persistentQueuePublisher_recoversAfterMessageSpoolCliShutdown}
 *       - <b>Bug reproducer.</b> Persistent publishing to a Queue destination + a broker-level
 *       {@code hardware message-spool shutdown} / {@code no shutdown} cycle driven through
 *       the broker's CLI via {@code docker exec}. This is the only mechanism that actually
 *       fans out unsolicited {@code CloseFlow} (503 Service Unavailable) to every bound
 *       publisher flow without dropping the JCSMP session - matching the customer's
 *       traceback exactly. The first post-shutdown publish surfaces
 *       {@link StaleSessionException}; on master the binding stays stuck in that state. After
 *       the {@code JCSMPOutboundMessageHandler} recreate-on-stale fix lands, the handler
 *       rebuilds the producer at the top of {@code handleMessage(...)} and the next publish
 *       succeeds.</li>
 *
 *   <li>{@link #test_persistentQueuePublisher_recoversAcrossRepeatedMessageSpoolCliShutdowns persistentQueuePublisher_recoversAcrossRepeatedMessageSpoolCliShutdowns}
 *       - <b>Repeated bug reproducer.</b> Same disruption as (4) but looped across multiple
 *       consecutive shutdown/restore cycles on the same binding. Proves the
 *       {@code producerNeedsRecreation} flag resets correctly after each successful
 *       recreation and that no state accumulates on the binder. A regression that only reset
 *       the flag once (e.g. a non-volatile read or a missed write in the success path) would
 *       reproduce the bug from the second cycle onwards.</li>
 * </ol>
 *
 * <p>All five tests run in {@link ExecutionMode#SAME_THREAD} because each touches shared
 * broker state (VPN-level spool quota, queue ingress/egress, broker-level message-spool)
 * that would briefly disrupt any concurrently-running tests on the same broker.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
class JCSMPProducerCloseFlowRecoveryIT {
	private static final Logger logger = LoggerFactory.getLogger(JCSMPProducerCloseFlowRecoveryIT.class);

	/**
	 * Control test (1/5) - persistent topic publisher + VPN spool toggle.
	 *
	 * <p>Documents the empirical broker behaviour: cycling {@code maxMsgSpoolUsage} through
	 * 0 does <em>not</em> tear down already-bound publisher flows on a topic destination,
	 * so the binding's producer stays healthy and subsequent persistent topic publishes
	 * succeed. This is the negative control for the bug reproducer below - if this test
	 * ever starts seeing {@link StaleSessionException}, the broker behaviour around spool
	 * quota changes has shifted and the bug surface area is wider than expected.
	 */
	@Test
	void test_persistentTopicPublisher_survivesSpoolToggle(
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String destination = RandomStringUtils.randomAlphanumeric(10);
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);
		DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(destination, moduleOutputChannel, producerProperties);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		BrokerConfigurator brokerConfig = BrokerConfiguratorBuilder.create(sempV2Api).build();
		Long originalMaxMsgSpoolUsageMb = brokerConfig.vpns().queryVpn(vpnName).getMaxMsgSpoolUsage();
		assertThat(originalMaxMsgSpoolUsageMb)
				.as("Captured maxMsgSpoolUsage should be a positive value")
				.isNotNull()
				.isPositive();

		boolean spoolRestored = false;
		try {
			moduleOutputChannel.send(MessageBuilder.withPayload("before-toggle").build());

			logger.info("Zeroing maxMsgSpoolUsage for VPN '{}'", vpnName);
			brokerConfig.vpns().disableMsgSpoolForVpn(vpnName);
			awaitVpnMaxMsgSpoolUsage(sempV2Api, vpnName, 0L);

			logger.info("Restoring maxMsgSpoolUsage={} MB for VPN '{}'", originalMaxMsgSpoolUsageMb, vpnName);
			brokerConfig.vpns().restoreMsgSpoolForVpn(vpnName, originalMaxMsgSpoolUsageMb);
			spoolRestored = true;
			awaitVpnMaxMsgSpoolUsage(sempV2Api, vpnName, originalMaxMsgSpoolUsageMb);

			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("after-toggle").build()))
					.as("Persistent topic publisher should be unaffected by a VPN spool quota toggle")
					.doesNotThrowAnyException();
		} finally {
			if (!spoolRestored) {
				try {
					brokerConfig.vpns().restoreMsgSpoolForVpn(vpnName, originalMaxMsgSpoolUsageMb);
				} catch (Exception cleanupError) {
					logger.warn("Failed to restore maxMsgSpoolUsage for VPN '{}' during cleanup", vpnName, cleanupError);
				}
			}
			producerBinding.unbind();
		}
	}

	/**
	 * Control test (2/5) - DIRECT topic publisher + VPN spool toggle.
	 *
	 * <p>Direct messages bypass the broker's spool subsystem entirely, so a spool quota
	 * toggle has no effect on a direct publisher. Uses the raw JCSMP session because
	 * the binder's {@code XMLMessageMapper} unconditionally promotes outbound messages
	 * to {@link DeliveryMode#PERSISTENT} and there is no producer-property switch for
	 * direct mode.
	 */
	@Test
	void test_directTopicPublisher_survivesSpoolToggle(
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api) throws Exception {
		String topicName = RandomStringUtils.randomAlphanumeric(10);
		Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
		XMLMessageProducer producer = jcsmpSession.getMessageProducer(new SimpleJCSMPEventHandler());

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		BrokerConfigurator brokerConfig = BrokerConfiguratorBuilder.create(sempV2Api).build();
		Long originalMaxMsgSpoolUsageMb = brokerConfig.vpns().queryVpn(vpnName).getMaxMsgSpoolUsage();
		assertThat(originalMaxMsgSpoolUsageMb)
				.as("Captured maxMsgSpoolUsage should be a positive value")
				.isNotNull()
				.isPositive();

		boolean spoolRestored = false;
		try {
			TextMessage warmup = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			warmup.setDeliveryMode(DeliveryMode.DIRECT);
			warmup.setText("before-toggle");
			producer.send(warmup, topic);

			logger.info("Zeroing maxMsgSpoolUsage for VPN '{}'", vpnName);
			brokerConfig.vpns().disableMsgSpoolForVpn(vpnName);
			awaitVpnMaxMsgSpoolUsage(sempV2Api, vpnName, 0L);

			logger.info("Restoring maxMsgSpoolUsage={} MB for VPN '{}'", originalMaxMsgSpoolUsageMb, vpnName);
			brokerConfig.vpns().restoreMsgSpoolForVpn(vpnName, originalMaxMsgSpoolUsageMb);
			spoolRestored = true;
			awaitVpnMaxMsgSpoolUsage(sempV2Api, vpnName, originalMaxMsgSpoolUsageMb);

			TextMessage after = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
			after.setDeliveryMode(DeliveryMode.DIRECT);
			after.setText("after-toggle");
			assertThatCode(() -> producer.send(after, topic))
					.as("Direct topic publisher should be unaffected by a VPN spool quota toggle (no spool involvement)")
					.doesNotThrowAnyException();
		} finally {
			if (!spoolRestored) {
				try {
					brokerConfig.vpns().restoreMsgSpoolForVpn(vpnName, originalMaxMsgSpoolUsageMb);
				} catch (Exception cleanupError) {
					logger.warn("Failed to restore maxMsgSpoolUsage for VPN '{}' during cleanup", vpnName, cleanupError);
				}
			}
			producer.close();
		}
	}

	/**
	 * Control test (3/5) - persistent queue publisher + queue ingress/egress toggle.
	 *
	 * <p>Documents the empirical broker behaviour: disabling and re-enabling a queue's
	 * ingress and egress does <em>not</em> fan out unsolicited {@code CloseFlow} on
	 * publisher flows bound to that queue. While ingress is disabled the broker NACKs
	 * incoming publishes asynchronously; once it is re-enabled the publisher flow remains
	 * bound and publishes resume cleanly. Asserts the publisher continues to publish
	 * before, during the cycle (post-restoration), and after.
	 *
	 * <p>If this test ever starts surfacing {@link StaleSessionException}, the broker
	 * behaviour around queue ingress/egress changes has shifted and the bug surface area
	 * is wider than today.
	 */
	@Test
	void test_persistentQueuePublisher_survivesQueueIngressEgressToggle(
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String queueName = RandomStringUtils.randomAlphanumeric(20);
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.getExtension().setDestinationType(DestinationType.QUEUE);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);
		DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(queueName, moduleOutputChannel, producerProperties);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		BrokerConfigurator brokerConfig = BrokerConfiguratorBuilder.create(sempV2Api).build();

		boolean queueRestored = false;
		try {
			moduleOutputChannel.send(MessageBuilder.withPayload("before-toggle").build());

			logger.info("Disabling ingress and egress on queue '{}' in VPN '{}'", queueName, vpnName);
			brokerConfig.queues().disableIngressOnQueue(vpnName, queueName);
			brokerConfig.queues().disableEgressOnQueue(vpnName, queueName);
			awaitQueueIngressEgress(sempV2Api, vpnName, queueName, false, false);

			logger.info("Re-enabling ingress and egress on queue '{}' in VPN '{}'", queueName, vpnName);
			brokerConfig.queues().reenableIngressOnQueue(vpnName, queueName);
			brokerConfig.queues().reenableEgressOnQueue(vpnName, queueName);
			queueRestored = true;
			awaitQueueIngressEgress(sempV2Api, vpnName, queueName, true, true);

			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("after-toggle").build()))
					.as("Persistent queue publisher should be unaffected by a queue ingress/egress toggle")
					.doesNotThrowAnyException();
		} finally {
			if (!queueRestored) {
				try {
					brokerConfig.queues().reenableIngressOnQueue(vpnName, queueName);
					brokerConfig.queues().reenableEgressOnQueue(vpnName, queueName);
				} catch (Exception cleanupError) {
					logger.warn("Failed to re-enable queue '{}' during cleanup", queueName, cleanupError);
				}
			}
			try {
				producerBinding.unbind();
			} finally {
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				try {
					jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
				} catch (Exception deprovisionError) {
					logger.warn("Failed to deprovision queue '{}' during cleanup", queueName, deprovisionError);
				}
			}
		}
	}

	/**
	 * Bug reproducer (4/5) - persistent queue publisher + broker-level message-spool shutdown.
	 *
	 * <p>This is the precise customer-reported reproduction. The {@code hardware message-spool
	 * shutdown} / {@code no shutdown} CLI sequence is the only mechanism that fans out an
	 * unsolicited {@code CloseFlow} (503 Service Unavailable) to every currently-bound
	 * publisher flow on the broker <em>without</em> dropping the JCSMP session. SEMPv2-only
	 * approaches (zeroing the spool quota, toggling queue ingress/egress) either NACK
	 * publishes asynchronously without invalidating the flow, or also drop the session as a
	 * side-effect - neither matches the customer's actual failure mode.
	 *
	 * <p>The CLI is reached by finding the running PubSub+ test container via the docker
	 * client that testcontainers already exposes, then {@code docker exec}ing the
	 * {@code /usr/sw/loads/currentload/bin/cli -A} script with the spool commands piped on
	 * stdin. We cannot avoid this layer of indirection because {@code PubSubPlusExtension}
	 * does not expose the container as a parameter, and SEMPv2 has no equivalent action.
	 *
	 * <p>Two assertions:
	 * <ol>
	 *   <li><b>Bug witness</b> - the first post-shutdown publish surfaces
	 *       {@link MessagingException} with a {@link JCSMPException}-rooted cause. The
	 *       customer's stack trace shows {@link StaleSessionException} wrapping a
	 *       {@code JCSMPTransportException("Received unsolicited CloseFlow ... 503:Service
	 *       Unavailable")}; depending on broker timing and JCSMP buffer state the test
	 *       sometimes surfaces the inner {@code JCSMPTransportException} directly as the
	 *       root cause instead. Both forms are manifestations of the same broker-driven
	 *       flow teardown - we accept either via the common {@link JCSMPException}
	 *       supertype.</li>
	 *   <li><b>Recovery</b> - the next publish completes cleanly only after the
	 *       {@code JCSMPOutboundMessageHandler} recreate-on-stale fix lands.</li>
	 * </ol>
	 */
	@Test
	void test_persistentQueuePublisher_recoversAfterMessageSpoolCliShutdown(
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String queueName = RandomStringUtils.randomAlphanumeric(20);
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.getExtension().setDestinationType(DestinationType.QUEUE);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);
		DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(queueName, moduleOutputChannel, producerProperties);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String solaceContainerId = findSolaceContainerId();
		boolean spoolRestored = false;
		try {
			moduleOutputChannel.send(MessageBuilder.withPayload("before-shutdown").build());

			// === Phase 1: broker-side disruption via CLI ===
			// Tear down the broker's spool subsystem so it fans out unsolicited CloseFlow
			// to every bound publisher / consumer flow. The JCSMP session stays connected;
			// only the GD flows die.
			//
			// NOTE on command shape: the Solace CLI requires stepping through the config
			// sub-modes one line at a time (`hardware`, then `message-spool`) - the dotted
			// form `hardware message-spool` is not reliably accepted. The `shutdown`
			// command itself is destructive and is guarded by TWO sequential confirmation
			// prompts:
			//     All message spooling will be stopped.
			//     Do you want to continue (y/n)?  <-- first prompt
			//     Do you want to continue (y/n)?  <-- second prompt
			// We answer both with `y`. Without those, the exec hangs at the first prompt
			// and the shutdown never actually takes effect. `no shutdown` (the re-enable)
			// is non-destructive and does not prompt.
			logger.info("Shutting down broker message-spool via CLI in container '{}'", solaceContainerId);
			runSolaceCliCommands(solaceContainerId,
					"enable",
					"configure",
					"hardware",
					"message-spool",
					"shutdown",
					"y");
			// Wait until the broker's SEMP API is responsive again before driving the
			// re-enable CLI command. The shutdown is synchronous from the CLI's perspective,
			// but the broker's HTTP management surface may briefly stutter as the spool
			// subsystem transitions; awaitBrokerSempResponsive(...) probes that surface
			// instead of trusting a fixed sleep window.
			awaitBrokerSempResponsive(sempV2Api, vpnName);

			logger.info("Re-enabling broker message-spool via CLI in container '{}'", solaceContainerId);
			runSolaceCliCommands(solaceContainerId,
					"enable",
					"configure",
					"hardware",
					"message-spool",
					"no shutdown");
			spoolRestored = true;
			// Same probe again after re-enable - confirms the broker is back to a
			// queryable / stable state before we attempt the bug-witness publish.
			awaitBrokerSempResponsive(sempV2Api, vpnName);

			// === Phase 2: assertion 1 - bug witness ===
			// The first post-shutdown publish must surface a JCSMP-rooted MessagingException.
			// We accept any JCSMPException root cause (StaleSessionException,
			// ClosedFacilityException, JCSMPTransportException) because the broker's CLI-driven
			// teardown can surface as either the outer StaleSessionException form (matching
			// the customer's stack trace verbatim) or the inner JCSMPTransportException form
			// when JCSMP delivers the CloseFlow event before the producer's stale-marker has
			// been propagated to the send() caller. Both forms prove the same thing - that
			// the broker tore down the publisher flow and the binding's local producer is
			// no longer usable.
			logger.info("Attempting first post-shutdown publish; expecting a JCSMP-rooted failure");
			Awaitility.await("post-CLI-shutdown publish surfaces a JCSMP-rooted failure")
					.atMost(Duration.ofSeconds(20))
					.pollInterval(Duration.ofMillis(500))
					.untilAsserted(() -> assertThatThrownBy(() ->
							moduleOutputChannel.send(MessageBuilder.withPayload("witness-" + System.nanoTime()).build()))
							.isInstanceOf(MessagingException.class)
							.hasRootCauseInstanceOf(JCSMPException.class));

			// === Phase 3: assertion 2 - recovery ===
			//   Pre-fix (master): handler-local producer is still the dead one, this throws
			//                     StaleSessionException - assertion fails. That failure IS the bug.
			//   Post-fix:         handler detects the stale flag, recreates the producer and
			//                     the publish succeeds.
			logger.info("Attempting recovery publish; expecting success only with the fix in place");
			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("recovery").build()))
					.as("Publish after broker message-spool CLI shutdown must recover; on master it stays stale and re-throws StaleSessionException")
					.doesNotThrowAnyException();
		} finally {
			// Belt-and-suspenders: if assertion 1 timed out before we re-enabled the spool,
			// turn it back on so subsequent tests / the broker container stay healthy.
			if (!spoolRestored) {
				try {
					runSolaceCliCommands(solaceContainerId,
							"enable",
							"configure",
							"hardware",
							"message-spool",
							"no shutdown");
				} catch (Exception cleanupError) {
					logger.warn("Failed to re-enable broker message-spool during cleanup", cleanupError);
				}
			}
			try {
				producerBinding.unbind();
			} finally {
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				try {
					jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
				} catch (Exception deprovisionError) {
					logger.warn("Failed to deprovision queue '{}' during cleanup", queueName, deprovisionError);
				}
			}
		}
	}

	/**
	 * Repeated bug reproducer (5/5) - persistent queue publisher + N consecutive
	 * broker-level message-spool shutdown / restore cycles on the <em>same</em> binding.
	 *
	 * <p>Test 4 proves that the {@code JCSMPOutboundMessageHandler} fix recovers from a
	 * single unsolicited {@code CloseFlow}. This test proves the same fix continues to work
	 * across multiple consecutive stale-flow events on the same binding, without any
	 * intermediate {@code stop() / start()}. Concretely it asserts the
	 * {@code producerNeedsRecreation} flag is correctly cleared after each successful
	 * recreation - if it weren't, the second cycle's bug-witness assertion would never see
	 * a fresh stale exception (the recreated producer from cycle 1 is still considered
	 * "the new producer" until something tells the handler otherwise).
	 *
	 * <p>This is what guards against a class of regression where the flag-reset is moved or
	 * weakened (e.g. reset only on {@code start()}, or reset on the recreation-attempt
	 * branch but missed on the recreation-success branch). The unit test
	 * {@code test_recreationFlagResetAcrossStopStartCycle} covers the lifecycle-driven
	 * reset; this IT covers the per-event reset against a real broker.
	 *
	 * <p>Three cycles is enough to expose state-accumulation bugs while keeping the test
	 * runtime bounded: each cycle costs ~1 broker spool restart (~5-10s).
	 */
	@Test
	void test_persistentQueuePublisher_recoversAcrossRepeatedMessageSpoolCliShutdowns(
			JCSMPSession jcsmpSession,
			SempV2Api sempV2Api,
			SpringCloudStreamContext context,
			TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();

		String queueName = RandomStringUtils.randomAlphanumeric(20);
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.getExtension().setDestinationType(DestinationType.QUEUE);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);
		DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

		Binding<MessageChannel> producerBinding = binder.bindProducer(queueName, moduleOutputChannel, producerProperties);

		String vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		String solaceContainerId = findSolaceContainerId();
		final int cycles = 3;
		boolean spoolRestored = true;
		try {
			moduleOutputChannel.send(MessageBuilder.withPayload("initial-healthy").build());

			for (int cycle = 1; cycle <= cycles; cycle++) {
				logger.info("=== Cycle {}/{}: shutdown -> witness-failure -> recover ===", cycle, cycles);

				// Mark the spool as "down" before issuing the destructive CLI; cleared again
				// once the matching `no shutdown` lands. If the witness assertion below times
				// out mid-cycle the finally-block sees this flag and re-enables the spool.
				spoolRestored = false;

				runSolaceCliCommands(solaceContainerId,
						"enable",
						"configure",
						"hardware",
						"message-spool",
						"shutdown",
						"y");
				awaitBrokerSempResponsive(sempV2Api, vpnName);

				runSolaceCliCommands(solaceContainerId,
						"enable",
						"configure",
						"hardware",
						"message-spool",
						"no shutdown");
				spoolRestored = true;
				awaitBrokerSempResponsive(sempV2Api, vpnName);

				// Bug witness for THIS cycle. The handler is still holding a producer
				// (either the original one in cycle 1, or the one recreated by cycle N-1's
				// recovery publish in later cycles); the broker just tore it down. The
				// next publish must surface a JCSMP-rooted failure - if it doesn't, the
				// flag was never re-armed and the handler is no longer noticing
				// stale-flow events after the first one.
				final int currentCycle = cycle;
				Awaitility.await(String.format("cycle %d: post-shutdown publish surfaces JCSMP failure", currentCycle))
						.atMost(Duration.ofSeconds(20))
						.pollInterval(Duration.ofMillis(500))
						.untilAsserted(() -> assertThatThrownBy(() ->
								moduleOutputChannel.send(MessageBuilder.withPayload(
										"witness-c" + currentCycle + "-" + System.nanoTime()).build()))
								.isInstanceOf(MessagingException.class)
								.hasRootCauseInstanceOf(JCSMPException.class));

				// Recovery for THIS cycle. The fix must take effect on every cycle, not
				// just the first - any regression that resets the flag only once would
				// leave the producer permanently dead from cycle 2 onwards.
				assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload(
						"recovery-c" + currentCycle).build()))
						.as("Cycle %d: publish must recover after broker CLI shutdown; producerNeedsRecreation flag must reset between cycles", currentCycle)
						.doesNotThrowAnyException();
			}

			// After N cycles the binding's producer should still be servicing plain publishes
			// without re-triggering the recreate path - i.e. the flag is cleanly cleared and
			// the handler is in a steady state, not stuck in a perpetual recreate loop.
			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("final-healthy").build()))
					.as("After %d shutdown/recover cycles, the binding's producer must continue to publish normally", cycles)
					.doesNotThrowAnyException();
		} finally {
			if (!spoolRestored) {
				try {
					runSolaceCliCommands(solaceContainerId,
							"enable",
							"configure",
							"hardware",
							"message-spool",
							"no shutdown");
				} catch (Exception cleanupError) {
					logger.warn("Failed to re-enable broker message-spool during cleanup", cleanupError);
				}
			}
			try {
				producerBinding.unbind();
			} finally {
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				try {
					jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
				} catch (Exception deprovisionError) {
					logger.warn("Failed to deprovision queue '{}' during cleanup", queueName, deprovisionError);
				}
			}
		}
	}

	// === SEMP-driven broker-state poll helpers ===
	// Each helper Awaits the broker reaching a known state before the test proceeds.
	// Polls SEMP every 100ms with a 10s ceiling. ignoreExceptions() lets the poll
	// shrug off transient ApiExceptions during state transitions (e.g. while the
	// broker is mid-restart from a CLI message-spool shutdown).

	/**
	 * Polls SEMPv2 until the VPN's {@code maxMsgSpoolUsage} matches the supplied value.
	 * Used after {@code BrokerConfigurator.disableMsgSpoolForVpn / restoreMsgSpoolForVpn}
	 * to confirm the broker has committed the change before the next test step runs.
	 */
	private static void awaitVpnMaxMsgSpoolUsage(SempV2Api sempV2Api, String vpnName, long expectedMb) {
		Awaitility.await(String.format("VPN '%s' maxMsgSpoolUsage == %d MB", vpnName, expectedMb))
				.atMost(Duration.ofSeconds(10))
				.pollInterval(Duration.ofMillis(100))
				.ignoreExceptions()
				.untilAsserted(() -> assertThat(sempV2Api.config()
						.getMsgVpn(vpnName, null, null)
						.getData()
						.getMaxMsgSpoolUsage())
						.isEqualTo(expectedMb));
	}

	/**
	 * Polls SEMPv2 until a queue's ingress and egress flags match the supplied values.
	 * Used after {@code BrokerConfigurator.disableIngressOnQueue / disableEgressOnQueue}
	 * (and their re-enable counterparts) to confirm both broker-side flags have settled.
	 */
	private static void awaitQueueIngressEgress(SempV2Api sempV2Api, String vpnName, String queueName,
												boolean expectedIngress, boolean expectedEgress) {
		Awaitility.await(String.format("queue '%s' ingress=%s, egress=%s",
						queueName, expectedIngress, expectedEgress))
				.atMost(Duration.ofSeconds(10))
				.pollInterval(Duration.ofMillis(100))
				.ignoreExceptions()
				.untilAsserted(() -> {
					ConfigMsgVpnQueue queue = sempV2Api.config()
							.getMsgVpnQueue(vpnName, queueName, null, null)
							.getData();
					assertThat(queue.isIngressEnabled()).as("ingress").isEqualTo(expectedIngress);
					assertThat(queue.isEgressEnabled()).as("egress").isEqualTo(expectedEgress);
				});
	}

	/**
	 * Polls SEMPv2 until the broker is responsive on the supplied VPN. Used as a proxy
	 * "broker is stable" check around the CLI message-spool shutdown / re-enable cycle
	 * where there is no direct SEMP-level signal for spool-subsystem state - if the
	 * monitor API answers a {@code MsgVpn} lookup successfully, the broker is at least
	 * back to a queryable state.
	 */
	private static void awaitBrokerSempResponsive(SempV2Api sempV2Api, String vpnName) {
		Awaitility.await("broker SEMP API responsive for VPN '" + vpnName + "'")
				.atMost(Duration.ofSeconds(15))
				.pollInterval(Duration.ofMillis(100))
				.ignoreExceptions()
				.untilAsserted(() -> assertThat(sempV2Api.monitor()
						.getMsgVpn(vpnName, null)
						.getData())
						.isNotNull());
	}

	// === Docker / CLI helpers (only used by the message-spool CLI shutdown test) ===

	/**
	 * Finds the running PubSub+ test container created by {@link PubSubPlusExtension}. Uses
	 * the docker-java client that testcontainers exposes (so it works on the same daemon
	 * testcontainers is using) and filters by image name. The standard Solace PubSub+
	 * container image identifier always contains {@code solace-pubsub-standard}.
	 */
	private static String findSolaceContainerId() {
		DockerClient docker = DockerClientFactory.instance().client();
		List<Container> containers = docker.listContainersCmd().exec();
		return containers.stream()
				.filter(c -> c.getImage() != null && c.getImage().contains("solace-pubsub-standard"))
				.findFirst()
				.map(Container::getId)
				.orElseThrow(() -> new IllegalStateException(
						"No running 'solace-pubsub-standard' container found via the docker client. " +
								"This test requires the PubSubPlusExtension to have provisioned its container."));
	}

	/**
	 * Drives the broker container's Solace CLI with the supplied commands.
	 *
	 * <p><b>Why this needs a TTY:</b> the Solace CLI detects whether stdin is a real
	 * terminal and only honours confirmation prompts (the "Do you want to continue
	 * (y/n)?" guard around destructive commands like {@code shutdown}) when it is. When
	 * stdin is a plain pipe (e.g. {@code printf '...' | cli -A}), the CLI silently
	 * cancels the destructive command and continues - which is why earlier attempts at
	 * piping {@code y} answers through a shell pipeline never actually shut the spool
	 * down. Allocating a pseudo-TTY on the {@code docker exec} (the docker-java
	 * equivalent of {@code docker exec -it}) makes the CLI process the prompts the
	 * same way it does in a real terminal session.
	 *
	 * <p>The commands are fed into the TTY's stdin via docker-java's
	 * {@link com.github.dockerjava.api.command.ExecStartCmd#withStdIn(java.io.InputStream)}.
	 * Trailing {@code end} / {@code exit} are appended so the CLI cleanly leaves config
	 * mode and exits instead of waiting on more interactive input.
	 *
	 * <p>The CLI binary path {@code /usr/sw/loads/currentload/bin/cli} is the standard
	 * location inside the {@code solace-pubsub-standard} image.
	 */
	private static void runSolaceCliCommands(String containerId, String... cliCommands) throws Exception {
		DockerClient docker = DockerClientFactory.instance().client();

		StringBuilder script = new StringBuilder();
		for (String cmd : cliCommands) {
			// Use CRLF so the input is accepted regardless of whether the pty is in
			// cooked or raw line discipline. Cooked mode treats \n as Enter on its
			// own, but appending \r is harmless and avoids ambiguity.
			script.append(cmd).append("\r\n");
		}
		// Terminators so the CLI returns instead of waiting for more interactive input.
		script.append("end\r\n").append("exit\r\n");

		ExecCreateCmdResponse exec = docker.execCreateCmd(containerId)
				.withTty(true)              // pseudo-TTY so the CLI honors confirmation prompts
				.withAttachStdin(true)
				.withAttachStdout(true)
				.withAttachStderr(true)
				.withCmd("/usr/sw/loads/currentload/bin/cli", "-A")
				.exec();

		ByteArrayInputStream stdin = new ByteArrayInputStream(
				script.toString().getBytes(StandardCharsets.UTF_8));

		docker.execStartCmd(exec.getId())
				.withTty(true)
				.withStdIn(stdin)
				.exec(new com.github.dockerjava.api.async.ResultCallback.Adapter<>())
				.awaitCompletion(30, TimeUnit.SECONDS);
	}
}