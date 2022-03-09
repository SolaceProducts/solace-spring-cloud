package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

@Component
public class SolaceSessionEventHandler implements SessionEventHandler {

    private final SolaceBinderHealthIndicator solaceBinderHealthIndicator;
    private static final Log logger = LogFactory.getLog(SolaceSessionEventHandler.class);

    public SolaceSessionEventHandler(SolaceBinderHealthIndicator solaceBinderHealthIndicator) {
        this.solaceBinderHealthIndicator = solaceBinderHealthIndicator;
    }

    @Override
    public void handleEvent(SessionEventArgs sessionEvent) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Received Solace session event %s.", sessionEvent));
        }
        if (sessionEvent.getEvent() == SessionEvent.DOWN_ERROR) {
            solaceBinderHealthIndicator.down(sessionEvent.getException(), sessionEvent.getResponseCode(), sessionEvent.getInfo());
        } else if (sessionEvent.getEvent() == SessionEvent.RECONNECTING) {
            solaceBinderHealthIndicator.reconnecting();
        } else if (sessionEvent.getEvent() == SessionEvent.RECONNECTED) {
            solaceBinderHealthIndicator.up();
        }
    }

    public void connected() {
        solaceBinderHealthIndicator.up();
    }
}
