package com.solace.spring.cloud.stream.binder.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceSessionEventHandler;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
    initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
class DefaultSolaceSessionManagerIT {

  private JCSMPProperties jcsmpProperties;
  private @Mock SolaceSessionEventHandler solaceSessionEventHandler;

  @BeforeEach
  void setUp(JCSMPProperties jcsmpProperties) {
    this.jcsmpProperties = jcsmpProperties;
  }

  @Test
  void testGetSession() throws Exception {
    DefaultSolaceSessionManager sessionManager = new DefaultSolaceSessionManager(
        jcsmpProperties, null, solaceSessionEventHandler, null);
    assertFalse(sessionManager.getSession().isClosed());

    verify(solaceSessionEventHandler).setSessionHealthDown();
    verify(solaceSessionEventHandler).setSessionHealthUp();
  }

  @Test
  void testCloseSession() throws Exception {

    DefaultSolaceSessionManager sessionManager = new DefaultSolaceSessionManager(
        jcsmpProperties, null, solaceSessionEventHandler, null);
    JCSMPSession jcsmpSession = sessionManager.getSession();
    assertFalse(jcsmpSession.isClosed());

    sessionManager.close();

    assertTrue(jcsmpSession.isClosed());
    verify(solaceSessionEventHandler).setSessionHealthDown();
    verify(solaceSessionEventHandler).setSessionHealthUp();
  }

  @Test
  void testGetSessionConcurrent() throws Exception {
    DefaultSolaceSessionManager sessionManager = new DefaultSolaceSessionManager(
        jcsmpProperties, null, solaceSessionEventHandler, null);

    CountDownLatch latch = new CountDownLatch(10);
    List<JCSMPSession> sessions = new ArrayList<>();
    Runnable task = () -> {
      try {
        JCSMPSession session = sessionManager.getSession();
        sessions.add(session);
        latch.countDown();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    IntStream.range(0, 10).forEach(i -> {
      new Thread(task).start();
    });

    latch.await();

    IntStream.range(0, 10).forEach(i -> {
      assertSame(sessions.get(0), sessions.get(i)); //Should be the same instance
      assertFalse(sessions.get(i).isClosed());
    });

    sessionManager.close();
    verify(solaceSessionEventHandler).setSessionHealthDown();
    verify(solaceSessionEventHandler).setSessionHealthUp();
  }
}