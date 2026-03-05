package com.solace.spring.cloud.stream.binder.springBootTests.oauth2;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.time.Duration;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

public interface MessagingServiceFreeTierBrokerTestContainerWithTlsAndOAuthSetup {

  String FULL_DOCKER_COMPOSE_FILE_PATH = "src/test/resources/oauth2/free-tier-broker-with-tls-and-oauth-docker-compose.yml";
  String PUBSUB_BROKER_SERVICE_NAME = "solbroker";
  String NGINX_RPROXY_SERVICE_NAME = "solaceoauth";
  String KEYCLOAK_OAUTH_SERVICE_NAME = "keycloak";

  Logger LOGGER = LoggerFactory.getLogger(
      MessagingServiceFreeTierBrokerTestContainerWithTlsAndOAuthSetup.class);

  ComposeContainer COMPOSE_CONTAINER = new ComposeContainer(
      new File(FULL_DOCKER_COMPOSE_FILE_PATH)).withPull(true)
      .withExposedService(PUBSUB_BROKER_SERVICE_NAME, 8080)
      .withExposedService(PUBSUB_BROKER_SERVICE_NAME, 55443)
      .withExposedService(PUBSUB_BROKER_SERVICE_NAME, 55555)

      .withExposedService(NGINX_RPROXY_SERVICE_NAME, 10443)
      .withExposedService(NGINX_RPROXY_SERVICE_NAME, 1080)

      .withExposedService(KEYCLOAK_OAUTH_SERVICE_NAME, 8080)

      .waitingFor(PUBSUB_BROKER_SERVICE_NAME,
          Wait.forHttp("/").forPort(8080).withStartupTimeout(Duration.ofSeconds(120)))
      .waitingFor(NGINX_RPROXY_SERVICE_NAME,
          Wait.forHttp("/").forPort(10443).allowInsecure().usingTls()
              .withStartupTimeout(Duration.ofSeconds(120))).waitingFor(KEYCLOAK_OAUTH_SERVICE_NAME,
          Wait.forHttp("/").forPort(8080).allowInsecure()
              .withStartupTimeout(Duration.ofSeconds(120)));

  @BeforeAll
  static void startContainer() {
    String trustStorePath = new File(
        "src/test/resources/oauth2/certs/client/client-truststore.p12").getAbsolutePath();
    String trustStorePassword = "changeMe123";

    System.setProperty("javax.net.ssl.trustStore", trustStorePath);
    System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

    // Explicitly override the JVM default SSLContext so that Spring Security's RestClient
    // (used for OAuth2 token requests) trusts the test CA. This is necessary because
    // SSLContext.getDefault() may have been initialized before the system properties were set
    // (e.g., during Testcontainers Docker client initialization), caching a context that
    // does not include the test CA certificate.
    try {
      KeyStore trustStore = KeyStore.getInstance("PKCS12");
      try (FileInputStream fis = new FileInputStream(trustStorePath)) {
        trustStore.load(fis, trustStorePassword.toCharArray());
      }
      TrustManagerFactory tmf = TrustManagerFactory.getInstance(
          TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, tmf.getTrustManagers(), null);
      SSLContext.setDefault(sslContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to configure default SSL context for OAuth2 tests", e);
    }

    COMPOSE_CONTAINER.start();
  }

  @BeforeAll
  static void checkContainer() {
    String solaceBroker = COMPOSE_CONTAINER.getServiceHost(PUBSUB_BROKER_SERVICE_NAME, 8080);
    assertNotNull(solaceBroker, "solace broker host expected to be not null");

    String nginxProxy = COMPOSE_CONTAINER.getServiceHost(NGINX_RPROXY_SERVICE_NAME, 10443);
    assertNotNull(nginxProxy, "nginx proxy host expected to be not null");

    String keycloak = COMPOSE_CONTAINER.getServiceHost(KEYCLOAK_OAUTH_SERVICE_NAME, 8080);
    assertNotNull(keycloak, "keycloak host expected to be not null");
  }

  @AfterAll
  static void afterAll() {
    final SolaceBroker broker = SolaceBroker.getInstance();
    broker.backupFinalBrokerLogs(); //Backup container logs before it's destroyed
    COMPOSE_CONTAINER.stop();  //Destroy the container
  }

  class SolaceBroker {

    private static final class LazyHolder {

      static final SolaceBroker INSTANCE = new SolaceBroker();
    }


    public static SolaceBroker getInstance() {
      return LazyHolder.INSTANCE;
    }

    private final ComposeContainer container;

    private SolaceBroker(ComposeContainer container) {
      this.container = container;
    }

    public SolaceBroker() {
      this(COMPOSE_CONTAINER);
    }

    /**
     * bucks up final log form a broker
     */
    void backupFinalBrokerLogs() {
      final Consumer<ContainerState> copyToBrokerJob = containerState -> {
        if (containerState.isRunning()) {
          try {
            containerState.copyFileFromContainer("/usr/sw/jail/logs/debug.log",
                "oauth_test_final_debug.log");
            containerState.copyFileFromContainer("/usr/sw/jail/logs/event.log",
                "oauth_test_final_event.log");
          } catch (Exception e) {
            LOGGER.error("Failed to backup final log from a broker", e);
          }
        }
      };
      // run actual job on a container
      container.getContainerByServiceName(PUBSUB_BROKER_SERVICE_NAME + "_1")
          .ifPresent(copyToBrokerJob);
    }
  }
}