package com.solace.spring.cloud.stream.binder.springBootTests.oauth2;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder;
import com.solace.it.util.semp.config.BrokerConfiguratorBuilder.BrokerConfigurator;
import com.solace.it.util.semp.monitor.BrokerMonitorBuilder;
import com.solace.it.util.semp.monitor.BrokerMonitorBuilder.BrokerMonitor;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.action.ApiException;
import com.solace.test.integration.semp.v2.action.model.ActionMsgVpnClientDisconnect;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnAuthenticationOauthProfile;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnAuthenticationOauthProfile.OauthRoleEnum;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnClient;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.util.Files;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@ActiveProfiles("multibinderOAuth2")
@DirtiesContext //Ensures all listeners are stopped
class MultiBinderOAuth2IT implements
    MessagingServiceFreeTierBrokerTestContainerWithTlsAndOAuthSetup {

  private static final Logger logger = LoggerFactory.getLogger(MultiBinderOAuth2IT.class);
  private static BrokerConfigurator solaceConfigUtil;
  private static BrokerMonitor solaceMonitorUtil;
  private static SempV2Api sempV2Api;

  final static String OAUTH_PROFILE_NAME = "SolaceOauthResourceServer";
  final static String MSG_VPN_OAUTH_1 = "OAUTH_1";
  final static String MSG_VPN_OAUTH_2 = "OAUTH_2";

  final static String REALM_1 = "solace-oauth-resource-server1";
  final static String REALM_2 = "solace-oauth-resource-server2";

  @DynamicPropertySource
  static void registerDynamicProperties(DynamicPropertyRegistry registry) {
    String solaceHost = COMPOSE_CONTAINER.getServiceHost(PUBSUB_BROKER_SERVICE_NAME, 55443);
    int solaceSecureSMFPort = COMPOSE_CONTAINER.getServicePort(PUBSUB_BROKER_SERVICE_NAME, 55443);
    COMPOSE_CONTAINER.getServicePort(PUBSUB_BROKER_SERVICE_NAME, 55443);
    registry.add("spring.cloud.stream.binders.solace1.environment.solace.java.host",
        () -> String.format("tcps://%s:%s", solaceHost, solaceSecureSMFPort));
    registry.add("spring.cloud.stream.binders.solace2.environment.solace.java.host",
        () -> String.format("tcps://%s:%s", solaceHost, solaceSecureSMFPort));

    String nginxHost = COMPOSE_CONTAINER.getServiceHost(NGINX_RPROXY_SERVICE_NAME, 10443);
    int nginxSecurePort = COMPOSE_CONTAINER.getServicePort(NGINX_RPROXY_SERVICE_NAME, 10443);
    registry.add("spring.security.oauth2.client.provider.solace1-auth-server.token-uri",
        () -> String.format("https://%s:%s/auth/realms/%s/protocol/openid-connect/token", nginxHost,
            nginxSecurePort, REALM_1));
    registry.add("spring.security.oauth2.client.provider.solace2-auth-server.token-uri",
        () -> String.format("https://%s:%s/auth/realms/%s/protocol/openid-connect/token", nginxHost,
            nginxSecurePort, REALM_2));
  }

  @BeforeAll
  static void setUp() {
    try {
      String solaceHost = COMPOSE_CONTAINER.getServiceHost(PUBSUB_BROKER_SERVICE_NAME, 8080);
      int solaceSempPort = COMPOSE_CONTAINER.getServicePort(PUBSUB_BROKER_SERVICE_NAME, 8080);
      String sempUrl = String.format("http://%s:%s", solaceHost, solaceSempPort);
      sempV2Api = new SempV2Api(sempUrl, "admin", "admin");
      solaceConfigUtil = BrokerConfiguratorBuilder.create(sempV2Api).build();
      solaceMonitorUtil = BrokerMonitorBuilder.create(sempV2Api).build();

      solaceConfigUtil.vpns().copyVpn("default", MSG_VPN_OAUTH_1);
      solaceConfigUtil.vpns().updateClientProfile(MSG_VPN_OAUTH_1, "default");
      solaceConfigUtil.vpns().enableClientUsername(MSG_VPN_OAUTH_1, "default");

      solaceConfigUtil.vpns().copyVpn("default", MSG_VPN_OAUTH_2);
      solaceConfigUtil.vpns().updateClientProfile(MSG_VPN_OAUTH_2, "default");
      solaceConfigUtil.vpns().enableClientUsername(MSG_VPN_OAUTH_2, "default");

      logger.debug("Prepare to upload CA cert to the broker");
      final URL resource = MultiBinderOAuth2IT.class.getClassLoader()
          .getResource("oauth2/certs/rootCA/rootCA.pem");
      if (resource != null) {
        final File caFile = new File(resource.toURI());
        final String ca = Files.contentOf(caFile, StandardCharsets.US_ASCII);
        solaceConfigUtil.certAuthorities().setupCertAuthority("myCA", ca);
        logger.debug("CA cert is uploaded to the broker");
      } else {
        logger.error("CA cert file can't be uploaded");
        fail("Root certificate file can't be found");
      }

      //Setup Solace PubSub+ for OAuth2
      setupOAuth(MSG_VPN_OAUTH_1, REALM_1);
      setupOAuth(MSG_VPN_OAUTH_2, REALM_2);
    } catch (URISyntaxException e) {
      fail(e);
    }
  }

  private static void setupOAuth(String msgVpnName, String realmName) {
    solaceConfigUtil.vpns().enableOAuthAuth(msgVpnName);
    solaceConfigUtil.vpns().createOAuthProfile(msgVpnName, oAuthProfileResourceServer(realmName));
  }

  private static ConfigMsgVpnAuthenticationOauthProfile oAuthProfileResourceServer(
      String realmName) {
    final String AUTHORIZATION_GROUP_CLAIM_NAME = "";
    final String ENDPOINT_JWKS =
        "https://solaceoauth:10443/auth/realms/" + realmName + "/protocol/openid-connect/certs";
    final String ENDPOINT_USERINFO =
        "https://solaceoauth:10443/auth/realms/" + realmName + "/protocol/openid-connect/userinfo";
    final String REALM2_ISSUER_IDENTIFIER = "https://solaceoauth:10443/auth/realms/" + realmName;

    return new ConfigMsgVpnAuthenticationOauthProfile().enabled(true)
        .oauthProfileName(OAUTH_PROFILE_NAME)
        .authorizationGroupsClaimName(AUTHORIZATION_GROUP_CLAIM_NAME)
        .issuer(REALM2_ISSUER_IDENTIFIER).endpointJwks(ENDPOINT_JWKS)
        .endpointUserinfo(ENDPOINT_USERINFO).resourceServerParseAccessTokenEnabled(true)
        .resourceServerRequiredAudience("").resourceServerRequiredIssuer("")
        .resourceServerRequiredScope("").resourceServerValidateAudienceEnabled(false)
        .resourceServerValidateIssuerEnabled(false).resourceServerValidateScopeEnabled(false)
        .resourceServerValidateTypeEnabled(false).oauthRole(OauthRoleEnum.RESOURCE_SERVER);
  }

  @Test
  void checkHealthOfMultipleSolaceBinders(@Autowired MockMvc mvc) throws Exception {
    mvc.perform(get("/actuator/health"))
        .andExpectAll(status().isOk(), jsonPath("components.binders.components.solace1").exists(),
            jsonPath("components.binders.components.solace1.status").value("UP"),
            jsonPath("components.binders.components.solace2").exists(),
            jsonPath("components.binders.components.solace2.status").value("UP"));
  }

  @Test
  void checkHealthOfMultipleSolaceBindersWhenForceReconnect(@Autowired MockMvc mvc) {
    try {
      CountDownLatch forcedReconnect = new CountDownLatch(1);
      int numberOfReconnects = 5;
      AtomicBoolean failed = new AtomicBoolean(false);
      Thread t = new Thread(() -> {
        try {
          for (int i = 0; i < numberOfReconnects; i++) {
            try {
            String msgVPnName = i % 2 == 0 ? MSG_VPN_OAUTH_1 : MSG_VPN_OAUTH_2;
            MonitorMsgVpnClient msgVpnClient = solaceMonitorUtil.vpnClients()
                .queryVpnClients(msgVPnName)
                .stream()
                .filter(client -> client.getClientUsername().startsWith("default"))
                .findFirst().orElse(null);

            if (msgVpnClient == null) {
              throw new RuntimeException("Client not found");
            }

              logger.info("Forcing Session Reconnect for client: {}", msgVpnClient.getClientName());
              sempV2Api.action()
                  .doMsgVpnClientDisconnect(new ActionMsgVpnClientDisconnect(), msgVPnName,
                          msgVpnClient.getClientName());
            } catch (ApiException e) {
              throw new RuntimeException(e);
            } finally {
              Thread.sleep(10_000); //Introduced delay between force reconnects
            }
          }
        } catch (Exception e) {
          logger.error("Failed to force reconnect", e);
          failed.set(true);
        } finally {
          forcedReconnect.countDown();
        }
      });
      t.start();

      logger.info("Wait for force session reconnect, to refresh token");
      boolean success = forcedReconnect.await(3, TimeUnit.MINUTES);
      if (!success) {
        fail("Timed out waiting for token refresh");
      }

      assertFalse(failed.get(), "Failed to force reconnect");

      //Thread.sleep(10000); //Wait for reconnect to complete

      mvc.perform(get("/actuator/health"))
          .andExpectAll(status().isOk(), jsonPath("components.binders.components.solace1").exists(),
              jsonPath("components.binders.components.solace1.status").value("UP"),
              jsonPath("components.binders.components.solace2").exists(),
              jsonPath("components.binders.components.solace2.status").value("UP"));
    } catch (Exception e) {
      fail(e);
    }
  }
}