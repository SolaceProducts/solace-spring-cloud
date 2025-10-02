package com.solace.spring.cloud.stream.binder.properties;


import com.solace.spring.cloud.stream.binder.util.SessionInitializationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spring.cloud.stream.solace.binder")
public class SolaceBinderConfigurationProperties {


  private SessionInitializationMode sessionInitializationMode = SessionInitializationMode.EAGER;

  public SessionInitializationMode getSessionInitializationMode() {
    return sessionInitializationMode;
  }

  public void setSessionInitializationMode(SessionInitializationMode sessionInitializationMode) {
    this.sessionInitializationMode = sessionInitializationMode;
  }
}