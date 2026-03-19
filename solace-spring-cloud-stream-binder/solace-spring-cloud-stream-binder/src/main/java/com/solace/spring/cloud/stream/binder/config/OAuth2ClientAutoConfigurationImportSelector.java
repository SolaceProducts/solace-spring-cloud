package com.solace.spring.cloud.stream.binder.config;

import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

class OAuth2ClientAutoConfigurationImportSelector implements ImportSelector {

  private static final String OAUTH2_AUTO_CONFIG =
      "org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientAutoConfiguration";

  @Override
  public String[] selectImports(@NonNull AnnotationMetadata importingClassMetadata) {
    try {
      Class.forName(OAUTH2_AUTO_CONFIG);
      return new String[]{OAUTH2_AUTO_CONFIG};
    } catch (ClassNotFoundException e) {
      return new String[0];
    }
  }
}