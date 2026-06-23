package com.solace.spring.cloud.stream.binder.properties;


import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SessionInitializationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.cloud.stream.solace.binder")
public class SolaceBinderConfigurationProperties {


  private SessionInitializationMode sessionInitializationMode = SessionInitializationMode.EAGER;

  /**
   * Milliseconds to wait for a JCSMP producer close on binding stop before interrupting it
   * (DATAGO-137655); {@code producer.close()} blocks indefinitely while the session is reconnecting.
   */
  private long producerCloseTimeoutInMillis = JCSMPSessionProducerManager.DEFAULT_CLOSE_TIMEOUT_IN_MILLIS;

  public SessionInitializationMode getSessionInitializationMode() {
    return sessionInitializationMode;
  }

  public void setSessionInitializationMode(SessionInitializationMode sessionInitializationMode) {
    this.sessionInitializationMode = sessionInitializationMode;
  }

  public long getProducerCloseTimeoutInMillis() {
    return producerCloseTimeoutInMillis;
  }

  public void setProducerCloseTimeoutInMillis(long producerCloseTimeoutInMillis) {
    this.producerCloseTimeoutInMillis = producerCloseTimeoutInMillis;
  }
}
