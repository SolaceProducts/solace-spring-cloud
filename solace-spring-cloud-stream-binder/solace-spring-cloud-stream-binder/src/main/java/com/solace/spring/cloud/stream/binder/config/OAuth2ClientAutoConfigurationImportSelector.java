package com.solace.spring.cloud.stream.binder.config;

import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;

class OAuth2ClientAutoConfigurationImportSelector implements ImportSelector {

  private static final String OAUTH2_AUTO_CONFIG =
      "org.springframework.boot.security.oauth2.client.autoconfigure.OAuth2ClientAutoConfiguration";

  @Override
  public String[] selectImports(@NonNull AnnotationMetadata importingClassMetadata) {
    if (ClassUtils.isPresent(OAUTH2_AUTO_CONFIG, getClass().getClassLoader())) {
      return new String[]{OAUTH2_AUTO_CONFIG};
    }
    return new String[0];
  }
}