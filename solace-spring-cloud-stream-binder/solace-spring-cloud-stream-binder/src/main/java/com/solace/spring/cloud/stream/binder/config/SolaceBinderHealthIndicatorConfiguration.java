package com.solace.spring.cloud.stream.binder.config;

import com.solace.spring.cloud.stream.binder.util.SolaceBinderHealthIndicator;
import com.solace.spring.cloud.stream.binder.util.SolaceSessionEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.autoconfigure.health.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
@ConditionalOnEnabledHealthIndicator("binders")
public class SolaceBinderHealthIndicatorConfiguration {

    private static final Log logger = LogFactory.getLog(SolaceBinderHealthIndicatorConfiguration.class);

    @Bean
    public SolaceBinderHealthIndicator solaceBinderHealthIndicator() {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating Solace Binder Health Indicator");
        }
        return new SolaceBinderHealthIndicator();
    }

    @Bean
    public SolaceSessionEventHandler solaceSessionEventHandler(SolaceBinderHealthIndicator solaceBinderHealthIndicator) {
        if (logger.isDebugEnabled()) {
            logger.debug("Creating Solace Session Event Handler");
        }
        return new SolaceSessionEventHandler(solaceBinderHealthIndicator);
    }

}
