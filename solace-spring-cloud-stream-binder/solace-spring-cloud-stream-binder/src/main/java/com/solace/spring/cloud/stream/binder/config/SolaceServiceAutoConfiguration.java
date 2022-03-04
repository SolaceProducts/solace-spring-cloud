package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoCloudConfiguration;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ConditionalOnMissingBean(Binder.class)
@Import({ SolaceMessageChannelBinderConfiguration.class, SolaceJavaAutoCloudConfiguration.class, SolaceJavaAutoConfiguration.class })
public class SolaceServiceAutoConfiguration {
    
}
