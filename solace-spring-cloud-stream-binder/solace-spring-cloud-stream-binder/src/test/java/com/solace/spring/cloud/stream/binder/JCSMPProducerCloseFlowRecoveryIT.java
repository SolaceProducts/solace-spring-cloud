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
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLMessageProducer;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.testcontainers.DockerClientFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * DATAGO-134580 broker ITs: three control cases (spool toggle, direct publisher, queue
 * ingress/egress toggle) plus two recovery cases (single + repeated broker-level
 * {@code hardware message-spool shutdown}) proving the binding recovers from unsolicited
 * {@code CloseFlow}. Runs SAME_THREAD because each test mutates shared broker state.
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@Execution(ExecutionMode.SAME_THREAD)
class JCSMPProducerCloseFlowRecoveryIT {
	private static final Logger logger = LoggerFactory.getLogger(JCSMPProducerCloseFlowRecoveryIT.class);

	private JCSMPSession jcsmpSession;
	private SempV2Api sempV2Api;
	private SpringCloudStreamContext context;
	private String vpnName;
	private BrokerConfigurator brokerConfig;
	private Long originalMaxMsgSpoolUsageMb;

	// Per-test artifacts cleaned up in @AfterEach.
	private Binding<MessageChannel> producerBinding;
	private XMLMessageProducer rawProducer;
	private String provisionedQueueName;
	private boolean spoolModified;
	private boolean brokerSpoolNeedsRestore;
	private String solaceContainerId;

	@BeforeEach
	void setUp(JCSMPSession jcsmpSession, SempV2Api sempV2Api, SpringCloudStreamContext context) {
		this.jcsmpSession = jcsmpSession;
		this.sempV2Api = sempV2Api;
		this.context = context;
		this.vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		this.brokerConfig = BrokerConfiguratorBuilder.create(sempV2Api).build();
		this.originalMaxMsgSpoolUsageMb = brokerConfig.vpns().getMsgVpnSpool(vpnName);
		assertThat(originalMaxMsgSpoolUsageMb)
				.as("Captured maxMsgSpoolUsage should be a positive value")
				.isNotNull()
				.isPositive();
	}

	@AfterEach
	void tearDown() {
		if (spoolModified) {
			try {
				brokerConfig.vpns().setMsgVpnSpool(vpnName, originalMaxMsgSpoolUsageMb);
			} catch (Exception e) {
				logger.warn("Failed to restore maxMsgSpoolUsage for VPN '{}' during cleanup", vpnName, e);
			}
		}
		if (brokerSpoolNeedsRestore) {
			try {
				runSolaceCliCommands(solaceContainerId,
						"enable",
						"configure",
						"hardware",
						"message-spool",
						"no shutdown");
			} catch (Exception e) {
				logger.warn("Failed to re-enable broker message-spool during cleanup", e);
			}
		}
		if (rawProducer != null) {
			rawProducer.close();
		}
		if (producerBinding != null) {
			producerBinding.unbind();
		}
		if (provisionedQueueName != null) {
			Queue queue = JCSMPFactory.onlyInstance().createQueue(provisionedQueueName);
			try {
				jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			} catch (Exception e) {
				logger.warn("Failed to deprovision queue '{}' during cleanup", provisionedQueueName, e);
			}
		}
	}

	/** Control: persistent topic publisher survives a VPN spool quota toggle. */
	@Test
	void testPersistentTopicPublisherSurvivesSpoolToggle(TestInfo testInfo) throws Exception {
		DirectChannel moduleOutputChannel = createPersistentProducerChannel(
				RandomStringUtils.randomAlphanumeric(10), DestinationType.TOPIC, testInfo);

		moduleOutputChannel.send(MessageBuilder.withPayload("before-toggle").build());

		logger.info("Zeroing maxMsgSpoolUsage for VPN '{}'", vpnName);
		spoolModified = true;
		brokerConfig.vpns().setMsgVpnSpool(vpnName, 0L);

		logger.info("Restoring maxMsgSpoolUsage={} MB for VPN '{}'", originalMaxMsgSpoolUsageMb, vpnName);
		brokerConfig.vpns().setMsgVpnSpool(vpnName, originalMaxMsgSpoolUsageMb);
		spoolModified = false;

		assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("after-toggle").build()))
				.as("Persistent topic publisher should be unaffected by a VPN spool quota toggle")
				.doesNotThrowAnyException();
	}

	/** Control: direct topic publisher (raw JCSMP) is unaffected by spool toggle. */
	@Test
	void testDirectTopicPublisherSurvivesSpoolToggle() throws Exception {
		String topicName = RandomStringUtils.randomAlphanumeric(10);
		Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
		rawProducer = jcsmpSession.getMessageProducer(new SimpleJCSMPEventHandler());

		TextMessage warmup = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		warmup.setDeliveryMode(DeliveryMode.DIRECT);
		warmup.setText("before-toggle");
		rawProducer.send(warmup, topic);

		logger.info("Zeroing maxMsgSpoolUsage for VPN '{}'", vpnName);
		spoolModified = true;
		brokerConfig.vpns().setMsgVpnSpool(vpnName, 0L);

		logger.info("Restoring maxMsgSpoolUsage={} MB for VPN '{}'", originalMaxMsgSpoolUsageMb, vpnName);
		brokerConfig.vpns().setMsgVpnSpool(vpnName, originalMaxMsgSpoolUsageMb);
		spoolModified = false;

		TextMessage after = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		after.setDeliveryMode(DeliveryMode.DIRECT);
		after.setText("after-toggle");
		assertThatCode(() -> rawProducer.send(after, topic))
				.as("Direct topic publisher should be unaffected by a VPN spool quota toggle (no spool involvement)")
				.doesNotThrowAnyException();
	}

	/** Control: persistent queue publisher survives a queue ingress/egress toggle. */
	@Test
	void testPersistentQueuePublisherSurvivesQueueIngressEgressToggle(TestInfo testInfo) throws Exception {
		String queueName = RandomStringUtils.randomAlphanumeric(20);
		provisionedQueueName = queueName;
		DirectChannel moduleOutputChannel = createPersistentProducerChannel(queueName, DestinationType.QUEUE, testInfo);

		moduleOutputChannel.send(MessageBuilder.withPayload("before-toggle").build());

		logger.info("Disabling ingress and egress on queue '{}' in VPN '{}'", queueName, vpnName);
		brokerConfig.queues().disableIngressOnQueue(vpnName, queueName);
		brokerConfig.queues().disableEgressOnQueue(vpnName, queueName);

		logger.info("Re-enabling ingress and egress on queue '{}' in VPN '{}'", queueName, vpnName);
		brokerConfig.queues().reenableIngressOnQueue(vpnName, queueName);
		brokerConfig.queues().reenableEgressOnQueue(vpnName, queueName);

		assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("after-toggle").build()))
				.as("Persistent queue publisher should be unaffected by a queue ingress/egress toggle")
				.doesNotThrowAnyException();
	}

	/**
	 * Recovery: broker-level {@code hardware message-spool shutdown} fans unsolicited
	 * CloseFlow to the bound producer; the handler's proactive {@code isClosed()} pre-check
	 * must rebuild and the first post-shutdown publish must succeed.
	 */
	@Test
	void testPersistentQueuePublisherRecoversAfterMessageSpoolCliShutdown(TestInfo testInfo) throws Exception {
		String queueName = RandomStringUtils.randomAlphanumeric(20);
		provisionedQueueName = queueName;
		DirectChannel moduleOutputChannel = createPersistentProducerChannel(queueName, DestinationType.QUEUE, testInfo);
		solaceContainerId = findSolaceContainerId();

		moduleOutputChannel.send(MessageBuilder.withPayload("before-shutdown").build());

		// CLI sub-modes are entered one line at a time; the `shutdown` prompt requires `y`.
		logger.info("Shutting down broker message-spool via CLI in container '{}'", solaceContainerId);
		brokerSpoolNeedsRestore = true;
		runSolaceCliCommands(solaceContainerId,
				"enable",
				"configure",
				"hardware",
				"message-spool",
				"shutdown",
				"y");
		awaitBrokerSempResponsive(sempV2Api, vpnName);

		logger.info("Re-enabling broker message-spool via CLI in container '{}'", solaceContainerId);
		runSolaceCliCommands(solaceContainerId,
				"enable",
				"configure",
				"hardware",
				"message-spool",
				"no shutdown");
		brokerSpoolNeedsRestore = false;
		awaitBrokerSempResponsive(sempV2Api, vpnName);

		logger.info("Attempting first post-shutdown publish; expecting proactive recovery");
		assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("recovery-1").build()))
				.as("First publish after broker CLI message-spool shutdown must recover via proactive isClosed() pre-check")
				.doesNotThrowAnyException();

		assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("recovery-2").build()))
				.as("Steady-state publish after recovery must continue to work")
				.doesNotThrowAnyException();
	}

	/** Recovery: same disruption looped 3x on the same binding to catch state-accumulation regressions. */
	@Test
	void testPersistentQueuePublisherRecoversAcrossRepeatedMessageSpoolCliShutdowns(TestInfo testInfo) throws Exception {
		String queueName = RandomStringUtils.randomAlphanumeric(20);
		provisionedQueueName = queueName;
		DirectChannel moduleOutputChannel = createPersistentProducerChannel(queueName, DestinationType.QUEUE, testInfo);
		solaceContainerId = findSolaceContainerId();
		final int cycles = 3;

		moduleOutputChannel.send(MessageBuilder.withPayload("initial-healthy").build());

		for (int cycle = 1; cycle <= cycles; cycle++) {
			logger.info("=== Cycle {}/{}: shutdown -> witness-failure -> recover ===", cycle, cycles);
			brokerSpoolNeedsRestore = true;

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
			brokerSpoolNeedsRestore = false;
			awaitBrokerSempResponsive(sempV2Api, vpnName);

			final int currentCycle = cycle;
			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload(
					"recovery-c" + currentCycle).build()))
					.as("Cycle %d: first publish after broker CLI shutdown must recover via proactive isClosed() pre-check", currentCycle)
					.doesNotThrowAnyException();

			assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload(
					"steady-c" + currentCycle).build()))
					.as("Cycle %d: steady-state publish after recovery must continue to work", currentCycle)
					.doesNotThrowAnyException();
		}

		assertThatCode(() -> moduleOutputChannel.send(MessageBuilder.withPayload("final-healthy").build()))
				.as("After %d shutdown/recover cycles, the binding's producer must continue to publish normally", cycles)
				.doesNotThrowAnyException();
	}

	private DirectChannel createPersistentProducerChannel(String destination, DestinationType destinationType,
														  TestInfo testInfo) throws Exception {
		SolaceTestBinder binder = context.getBinder();
		ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
		producerProperties.getExtension().setDestinationType(destinationType);
		BindingProperties producerBindingProperties = new BindingProperties();
		producerBindingProperties.setProducer(producerProperties);
		DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);
		producerBinding = binder.bindProducer(destination, moduleOutputChannel, producerProperties);
		return moduleOutputChannel;
	}

	/** Polls until the broker SEMP API answers a {@code MsgVpn} lookup again. */
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

	// Docker / CLI helpers (only used by the message-spool CLI shutdown tests).

	/** Finds the running {@code solace-pubsub-standard} container via the testcontainers docker client. */
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
	 * Runs the Solace CLI inside the broker container. Requires a pseudo-TTY so confirmation
	 * prompts (e.g. destructive {@code shutdown}) are honoured; trailing {@code end} + two
	 * {@code exit}s ensure {@code cli -A} terminates instead of hanging at the prompt.
	 */
	private static void runSolaceCliCommands(String containerId, String... cliCommands) throws Exception {
		DockerClient docker = DockerClientFactory.instance().client();

		StringBuilder script = new StringBuilder();
		for (String cmd : cliCommands) {
			script.append(cmd).append("\r\n");
		}
		script.append("end\r\n").append("exit\r\n").append("exit\r\n");

		ExecCreateCmdResponse exec = docker.execCreateCmd(containerId)
				.withTty(true)
				.withAttachStdin(true)
				.withAttachStdout(true)
				.withAttachStderr(true)
				.withCmd("/usr/sw/loads/currentload/bin/cli", "-A")
				.exec();

		ByteArrayInputStream stdin = new ByteArrayInputStream(
				script.toString().getBytes(StandardCharsets.UTF_8));

		final StringBuilder capturedOutput = new StringBuilder();
		com.github.dockerjava.api.async.ResultCallback.Adapter<com.github.dockerjava.api.model.Frame> callback =
				new com.github.dockerjava.api.async.ResultCallback.Adapter<>() {
					@Override
					public void onNext(com.github.dockerjava.api.model.Frame frame) {
						capturedOutput.append(new String(frame.getPayload(), StandardCharsets.UTF_8));
					}
				};

		boolean completed = docker.execStartCmd(exec.getId())
				.withTty(true)
				.withStdIn(stdin)
				.exec(callback)
				.awaitCompletion(30, TimeUnit.SECONDS);

		if (!completed) {
			throw new IllegalStateException(String.format(
					"Solace CLI exec did not complete within 30s in container '%s'. " +
							"Script:%n%s%nCaptured output:%n%s",
					containerId, script, capturedOutput));
		}
	}
}
