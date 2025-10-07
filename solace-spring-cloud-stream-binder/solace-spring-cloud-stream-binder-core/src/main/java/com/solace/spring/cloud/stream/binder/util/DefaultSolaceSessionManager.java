package com.solace.spring.cloud.stream.binder.util;

import static com.solacesystems.jcsmp.XMLMessage.Outcome.ACCEPTED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.FAILED;
import static com.solacesystems.jcsmp.XMLMessage.Outcome.REJECTED;

import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solacesystems.jcsmp.Context;
import com.solacesystems.jcsmp.ContextProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import com.solacesystems.jcsmp.impl.client.ClientInfoProvider;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

public class DefaultSolaceSessionManager implements SolaceSessionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSolaceSessionManager.class);

  private final JCSMPProperties jcsmpProperties;
  private final ClientInfoProvider clientInfoProvider;
  private final SolaceSessionEventHandler solaceSessionEventHandler;
  private final SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider;

  private volatile JCSMPSession jcsmpSession;
  private volatile Context context;
  private final Object sessionLock = new Object();

  public DefaultSolaceSessionManager(JCSMPProperties jcsmpProperties,
      @Nullable ClientInfoProvider clientInfoProvider,
      @Nullable SolaceSessionEventHandler eventHandler,
      @Nullable SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider) {
    this.jcsmpProperties = jcsmpProperties;
    this.clientInfoProvider = clientInfoProvider;
    this.solaceSessionEventHandler = eventHandler;
    this.solaceSessionOAuth2TokenProvider = solaceSessionOAuth2TokenProvider;
  }

  @Override
  public JCSMPSession getSession() throws JCSMPException {
    if (jcsmpSession == null) {
      createSessionIfNeeded();
    }
    return jcsmpSession;
  }

  private void createSessionIfNeeded() throws JCSMPException {
    synchronized (sessionLock) {
      if (jcsmpSession != null) {
        return;
      }

      JCSMPProperties solaceJcsmpProperties = (JCSMPProperties) this.jcsmpProperties.clone();
      if (this.clientInfoProvider != null) {
        solaceJcsmpProperties.setProperty(JCSMPProperties.CLIENT_INFO_PROVIDER, clientInfoProvider);
      }

      try {
        SpringJCSMPFactory springJCSMPFactory = new SpringJCSMPFactory(solaceJcsmpProperties,
            solaceSessionOAuth2TokenProvider);

        if (solaceSessionEventHandler != null) {
          LOGGER.debug("Registering Solace Session Event handler on session");
          context = springJCSMPFactory.createContext(new ContextProperties());
          jcsmpSession = springJCSMPFactory.createSession(context, solaceSessionEventHandler);
        } else {
          jcsmpSession = springJCSMPFactory.createSession();
        }

        if (solaceSessionEventHandler != null) {
          solaceSessionEventHandler.setSessionHealthDown();
        }

        LOGGER.info("Connecting JCSMP session {}", jcsmpSession.getSessionName());
        jcsmpSession.connect();

        if (solaceSessionEventHandler != null) {
          solaceSessionEventHandler.setSessionHealthUp();
        }

        // Check broker compatibility
        if (jcsmpSession instanceof JCSMPBasicSession session
            && !session.isRequiredSettlementCapable(Set.of(ACCEPTED, FAILED, REJECTED))) {
          LOGGER.warn("The connected Solace PubSub+ Broker is not compatible. It doesn't support message NACK capability. Consumer bindings will fail to start.");
        }

        LOGGER.info("Successfully created and connected Solace JCSMP session");
      } catch (JCSMPException e) {
        LOGGER.error("Failed to initialize Solace JCSMP session", e);
        throw e;
      }
    }
  }

  @Override
  public void close() {
    synchronized (sessionLock) {
      if (jcsmpSession != null) {
        try {
          LOGGER.info("Closing JCSMP session {}", jcsmpSession.getSessionName());
          jcsmpSession.closeSession();
        } catch (Exception e) {
          LOGGER.warn("Error closing JCSMP session", e);
        } finally {
          jcsmpSession = null;
        }
      }

      if (context != null) {
        try {
          context.destroy();
        } catch (Exception e) {
          LOGGER.warn("Error destroying JCSMP context", e);
        } finally {
          context = null;
        }
      }
    }
  }
}