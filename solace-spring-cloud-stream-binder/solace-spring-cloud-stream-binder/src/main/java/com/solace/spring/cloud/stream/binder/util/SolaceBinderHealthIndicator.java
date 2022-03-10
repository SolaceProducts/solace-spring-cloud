package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

public class SolaceBinderHealthIndicator implements HealthIndicator {

    private static final String STATUS_RECONNECTING = "RECONNECTING";
    private static final String INFO = "info";
    private static final String RESPONSE_CODE = "responseCode";

    private volatile Health healthStatus;

    private static final Log logger = LogFactory.getLog(SolaceBinderHealthIndicator.class);

    public void up() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Solace binder status is %s", Status.UP));
        }
        healthStatus = Health.up().build();
    }

    public void reconnecting() {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Solace binder status is %s", STATUS_RECONNECTING));
        }
        healthStatus = Health.status(STATUS_RECONNECTING).build();
    }

    public void down(Exception exception, int responseCode, String info) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Solace binder status is %s", Status.DOWN));
        }
        Health.Builder builder = Health.down();
        if (exception != null) builder.withException(exception);
        if (responseCode != 0) builder.withDetail(RESPONSE_CODE, responseCode);
        if (info != null && !info.isEmpty()) builder.withDetail(INFO, info);
        healthStatus = builder.build();
    }

    @Override
    public Health health() {
        return healthStatus;
    }
}
