package com.solace.spring.cloud.stream.binder.instrumentation.springBootTests;


import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.createTag;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.findTraces;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyBrokerReceiveSpans;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyBrokerSendSpans;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyConsumerInternalSpans;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyConsumerProcessSpans;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyConsumerReceiveSpans;
import static com.solace.spring.cloud.stream.binder.instrumentation.util.JaegerQueryUtil.verifyPublishSpans;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Ulimit;
import com.solace.it.util.semp.SempClientException;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder.BrokerConfigurator;
import com.solace.spring.cloud.stream.binder.instrumentation.springBootTests.app.MainApp;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnClientUsername;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnTelemetryProfile;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnTelemetryProfile.ReceiverAclConnectDefaultActionEnum;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnTelemetryProfileTraceFilter;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnTelemetryProfileTraceFilterSubscription;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnTelemetryProfileTraceFilterSubscription.SubscriptionSyntaxEnum;
import io.opentelemetry.proto.trace.v1.TracesData;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/*
Java system parameters for running the test:

-javaagent:/path/to/opentelemetry-javaagent.jar
-Dotel.javaagent.extensions=/path/to/solace-opentelemetry-jcsmp-integration-x.x.x.jar,/path/to/spring-cloud-stream-binder-solace-instrumentation-x.x.x.jar
-Dotel.instrumentation.common.default-enabled=false
-Dotel.javaagent.debug=true
-Dotel.traces.exporter=otlp
-Dotel.metrics.exporter=none
-Dotel.logs.exporter=none
-Dotel.exporter.otlp.protocol=grpc
-Dotel.resource.attributes=service.name=binder-instrumentation-test
-Dotel.service.name=binder-instrumentation-test
-Dotel.propagators=solace_jcsmp_tracecontext
-Dotel.exporter.otlp.endpoint=http://localhost:4317
-Dotel.bsp.schedule.delay=100
-Dotel.bsp.max.queue.size=2048
-Dotel.bsp.max.export.batch.size=5
-Dotel.bsp.export.timeout=10000
-Dotel.instrumentation.solace-opentelemetry-jcsmp-integration.enabled=true
-Dotel.instrumentation.spring-cloud-stream-binder-solace-instrumentation.enabled=true
*/

@Testcontainers
class SolaceBinderInstrumentationIT {

  private static final Logger log = LoggerFactory.getLogger(SolaceBinderInstrumentationIT.class);
  private static final String SERVICE_NAME = "binder-instrumentation-test";

  private static final String JAEGER_IMAGE = "jaegertracing/all-in-one:latest";
  private static final String SOLACE_IMAGE = "solace/solace-pubsub-standard:latest";
  private static final String OTEL_COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.96.0";
  private static String jaegerQueryServer = "";

  private static SempV2Api sempV2Api;
  private static BrokerConfigurator solaceConfigUtil;
  public static final String MSG_VPN = "default";
  public static final String TELEMETRY_PROFILE_NAME = "trace";
  public static final String TELEMETRY_PROFILE_TRACE_FILTER_NAME = "default";
  public static final String TELEMETRY_PROFILE_TRACE_FILTER_SUBSCRIPTION = ">";
  public static final String TELEMETRY_PROFILE_TRACE_FILTER_SUBSCRIPTION_QUEUES = "#P2P/QUE/>";
  public static final String TRACING_DEFAULT_CLIENT_PROFILE_AND_ACL_PROFILE_NAME =
      "#telemetry-" + TELEMETRY_PROFILE_NAME;

  private static final Network network = Network.newNetwork();

  private static final GenericContainer<?> jaeger = new GenericContainer<>(
      DockerImageName.parse(JAEGER_IMAGE)).withNetwork(network)
      .withNetworkAliases("jaeger-all-in-one").withEnv("COLLECTOR_OTLP_ENABLED", "true")
      .withExposedPorts(16686, 4317, 4318, 16685)
      .waitingFor(Wait.forHttp("/").forPort(16686).withStartupTimeout(Duration.ofSeconds(120)));

  private static final GenericContainer<?> solbroker = new GenericContainer<>(
      DockerImageName.parse(SOLACE_IMAGE)).withNetwork(network).withNetworkAliases("solbroker")
      .withEnv("username_admin_globalaccesslevel", "admin")
      .withEnv("username_admin_password", "admin")
      .withEnv("system_scaling_maxconnectioncount", "100")
      .withEnv("webmanager_redirecthttp_enable", "false")
      .withSharedMemorySize(2L * 1024 * 1024 * 1024) // 2GB
      .withExposedPorts(8080, 55555, 5672).withCreateContainerCmdModifier(cmd -> {
        cmd.withName("solbroker");
        cmd.getHostConfig().withUlimits(
            Arrays.asList(new Ulimit("memlock", -1L, -1L), new Ulimit("nofile", 2448L, 1048576L)));
      }).waitingFor(Wait.forHttp("/").forPort(8080).withStartupTimeout(Duration.ofSeconds(180)));

  private static final GenericContainer<?> otelCollector = new GenericContainer<>(
      DockerImageName.parse(OTEL_COLLECTOR_IMAGE)).withNetwork(network)
      .withNetworkAliases("otelcollector").withExposedPorts(13133, 4317)
      .withCreateContainerCmdModifier(cmd -> {
        cmd.withName("otelcollector");
        cmd.getHostConfig()
            .withPortBindings(new PortBinding(Ports.Binding.bindPort(4317), new ExposedPort(4317)),
                new PortBinding(Ports.Binding.bindPort(13133), new ExposedPort(13133)));
      })
      .withClasspathResourceMapping("otel-collector-config.yaml", "/etc/otel-collector-config.yaml",
          BindMode.READ_ONLY).withCommand("--config=/etc/otel-collector-config.yaml")
      .dependsOn(jaeger, solbroker)
      .waitingFor(Wait.forHttp("/").forPort(13133).withStartupTimeout(Duration.ofSeconds(120)));

  @BeforeAll
  static void setup() throws InterruptedException {
    solbroker.start();
    otelCollector.start();

    //Initialize SEMP client and configure broker
    String solaceHost = solbroker.getHost();
    Integer solaceSempPort = solbroker.getMappedPort(8080);
    Integer solaceSMFPort = solbroker.getMappedPort(55555);

    String sempUrl = String.format("http://%s:%s", solaceHost, solaceSempPort);
    sempV2Api = new SempV2Api(sempUrl, "admin", "admin");
    solaceConfigUtil = BrokerConfiguratorBuilder.create(sempV2Api).build();

    String smfUrl = String.format("tcp://%s:%s", solaceHost, solaceSMFPort);
    System.setProperty("spring.cloud.stream.binders.local-solace.environment.solace.java.host",
        smfUrl);

    String otelCollectorHost = otelCollector.getHost();
    Integer otelCollectorGrpcPort = otelCollector.getMappedPort(4317);
    String otelGrpcUrl = String.format("http://%s:%s", otelCollectorHost, otelCollectorGrpcPort);
    System.setProperty("otel.exporter.otlp.endpoint", otelGrpcUrl);

    Thread.sleep(3000);
    setupPubSubPlusTracing();
  }

  @AfterAll
  static void tearDown() {
    otelCollector.stop();
    solbroker.stop();
    network.close();
  }

  @BeforeEach
  void beforeEach() {
    jaeger.start();

    String jaegerHost = jaeger.getHost();
    Integer jaegerPort = jaeger.getMappedPort(16685);
    jaegerQueryServer = String.format("%s:%s", jaegerHost, jaegerPort);
  }

  @AfterEach
  void afterEach() {
    jaeger.stop();
  }

  private static void setupPubSubPlusTracing() {
    solaceConfigUtil.vpns().enableBasicAuth(MSG_VPN);

    try {
      final ConfigMsgVpnTelemetryProfile telemetryProfile = new ConfigMsgVpnTelemetryProfile().telemetryProfileName(
              MSG_VPN).telemetryProfileName(TELEMETRY_PROFILE_NAME)
          .receiverAclConnectDefaultAction(ReceiverAclConnectDefaultActionEnum.ALLOW)
          .traceEnabled(true).receiverEnabled(true);
      sempV2Api.config().createMsgVpnTelemetryProfile(telemetryProfile, MSG_VPN, null, null);
    } catch (ApiException e) {
      throw new SempClientException("Failed to create telemetry profile", e);
    }

    try {
      final ConfigMsgVpnTelemetryProfileTraceFilter traceFilter = new ConfigMsgVpnTelemetryProfileTraceFilter().msgVpnName(
              MSG_VPN).telemetryProfileName(TELEMETRY_PROFILE_NAME)
          .traceFilterName(TELEMETRY_PROFILE_TRACE_FILTER_NAME).enabled(true);
      sempV2Api.config()
          .createMsgVpnTelemetryProfileTraceFilter(traceFilter, MSG_VPN, TELEMETRY_PROFILE_NAME,
              null, null);
    } catch (ApiException e) {
      throw new SempClientException("Failed to create trace filter", e);
    }

    try {
      final String[] subscriptions = {TELEMETRY_PROFILE_TRACE_FILTER_SUBSCRIPTION,
          TELEMETRY_PROFILE_TRACE_FILTER_SUBSCRIPTION_QUEUES};

      for (String s : subscriptions) {
        final ConfigMsgVpnTelemetryProfileTraceFilterSubscription subscription = new ConfigMsgVpnTelemetryProfileTraceFilterSubscription().msgVpnName(
                MSG_VPN).telemetryProfileName(TELEMETRY_PROFILE_NAME)
            .traceFilterName(TELEMETRY_PROFILE_TRACE_FILTER_NAME)
            .subscriptionSyntax(SubscriptionSyntaxEnum.SMF).subscription(s);
        sempV2Api.config()
            .createMsgVpnTelemetryProfileTraceFilterSubscription(subscription, MSG_VPN,
                TELEMETRY_PROFILE_NAME, TELEMETRY_PROFILE_TRACE_FILTER_NAME, null, null);
      }
    } catch (ApiException e) {
      throw new SempClientException("Failed to create trace filter subscription", e);
    }

    try {
      final ConfigMsgVpnClientUsername clientUsername = new ConfigMsgVpnClientUsername().msgVpnName(
              MSG_VPN).clientUsername(TELEMETRY_PROFILE_NAME).password(TELEMETRY_PROFILE_NAME)
          .enabled(true).clientProfileName(TRACING_DEFAULT_CLIENT_PROFILE_AND_ACL_PROFILE_NAME)
          .aclProfileName(TRACING_DEFAULT_CLIENT_PROFILE_AND_ACL_PROFILE_NAME);
      sempV2Api.config().createMsgVpnClientUsername(clientUsername, MSG_VPN, null, null);
    } catch (ApiException e) {
      throw new SempClientException("Failed to create client username", e);
    }

    try {
      final ConfigMsgVpnClientUsername clientUsername = new ConfigMsgVpnClientUsername().msgVpnName(
          MSG_VPN).clientUsername("default").password("default").enabled(true);
      sempV2Api.config().updateMsgVpnClientUsername(clientUsername, MSG_VPN, "default", null, null);
    } catch (ApiException e) {
      throw new SempClientException("Failed to create client username", e);
    }
  }

  @Test
  void testPublisherInstrumentation() {
    final int numMsgs = 1;
    final String springProfile = "supplier";

    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfile).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 2);
      verifyPublishSpans(traces, numMsgs, "solace/supply/hello", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testConsumerInstrumentation() {
    final int numMsgs = 1;
    final String springProfile = "consumer";
    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfile).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 6);
      verifyPublishSpans(traces, numMsgs, "solace/supply/consumerQueue", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs);
      verifyBrokerSendSpans(traces, numMsgs);
      verifyConsumerReceiveSpans(traces, numMsgs, "consumerQueue", "queue",
          "solace/supply/consumerQueue");
      verifyConsumerProcessSpans(traces, numMsgs, "consumerQueue", "queue",
          "solace/supply/consumerQueue");
      verifyConsumerInternalSpans(traces, numMsgs);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testProcessorInstrumentation() {
    final int numMsgs = 1;
    final String springProfile = "processor";
    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfile).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 8);
      verifyPublishSpans(traces, numMsgs, "solace/supply/processorQueue", "topic");
      verifyBrokerSendSpans(traces, numMsgs);
      verifyConsumerReceiveSpans(traces, numMsgs, "processorQueue", "queue",
          "solace/supply/processorQueue");
      verifyConsumerProcessSpans(traces, numMsgs, "processorQueue", "queue",
          "solace/supply/processorQueue");
      verifyConsumerInternalSpans(traces, numMsgs);
      verifyPublishSpans(traces, numMsgs, "solace/processor/hello", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs * 2);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testDynamicDestinationInstrumentation() {
    final int numMsgs = 1;
    final String springProfile = "dynamicDestinationProcessor";
    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfile).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 8);
      verifyPublishSpans(traces, numMsgs, "solace/supply/dynamicDestQ", "topic");
      verifyBrokerSendSpans(traces, numMsgs);
      verifyConsumerReceiveSpans(traces, numMsgs, "dynamicDestQ", "queue",
          "solace/supply/dynamicDestQ");
      verifyConsumerProcessSpans(traces, numMsgs, "dynamicDestQ", "queue",
          "solace/supply/dynamicDestQ");
      verifyConsumerInternalSpans(traces, numMsgs);
      verifyPublishSpans(traces, numMsgs, "solace/dynamicDestination/hello", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs * 2);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testManualAckRedeliverInstrumentation() {
    final int numMsgs = 1;
    final int numDelivered = 3;
    final String[] springProfiles = {"manualAck", "requeue"};
    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfiles).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs,
          (numDelivered * numMsgs * 4) + 2);
      verifyPublishSpans(traces, numMsgs, "solace/supply/manualAckQueue", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs);
      verifyBrokerSendSpans(traces, numDelivered * numMsgs,
          createTag("messaging.solace.send.outcome", "delivery failed"));
      verifyConsumerReceiveSpans(traces, numDelivered * numMsgs, "manualAckQueue", "queue",
          "solace/supply/manualAckQueue");
      verifyConsumerProcessSpans(traces, numDelivered * numMsgs, "manualAckQueue", "queue",
          "solace/supply/manualAckQueue");
      verifyConsumerInternalSpans(traces, numDelivered * numMsgs);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testManualAckRejectInstrumentation() {
    final int numMsgs = 1;
    final String[] springProfiles = {"manualAck", "reject"};
    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfiles).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 6);
      verifyPublishSpans(traces, numMsgs, "solace/supply/manualAckQueue", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs);
      verifyBrokerSendSpans(traces, numMsgs,
          createTag("messaging.solace.send.outcome", "rejected"));
      verifyConsumerReceiveSpans(traces, numMsgs, "manualAckQueue", "queue",
          "solace/supply/manualAckQueue");
      verifyConsumerProcessSpans(traces, numMsgs, "manualAckQueue", "queue",
          "solace/supply/manualAckQueue");
      verifyConsumerInternalSpans(traces, numMsgs);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @CsvSource({"bindingErrorHandler", "globalErrorHandler"})
  void testBindingCustomErrorHandlerInstrumentation(String springProfile) {
    final int numMsgs = 1;
    final int maxAttempts = 3;

    try (var ignored = new SpringApplicationBuilder(MainApp.class).profiles(springProfile).run()) {
      log.info("Staring Application");

      List<TracesData> traces = findTraces(jaegerQueryServer, SERVICE_NAME, numMsgs, 8);
      verifyPublishSpans(traces, numMsgs, "solace/supply/errorHandlingTestQueue", "topic");
      verifyBrokerReceiveSpans(traces, numMsgs);
      verifyBrokerSendSpans(traces, numMsgs);
      verifyConsumerReceiveSpans(traces, numMsgs, "errorHandlingTestQueue", "queue",
          "solace/supply/errorHandlingTestQueue");
      verifyConsumerProcessSpans(traces, numMsgs, "errorHandlingTestQueue", "queue",
          "solace/supply/errorHandlingTestQueue");
      verifyConsumerInternalSpans(traces, numMsgs * maxAttempts);
      log.info("Stopping Application");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}