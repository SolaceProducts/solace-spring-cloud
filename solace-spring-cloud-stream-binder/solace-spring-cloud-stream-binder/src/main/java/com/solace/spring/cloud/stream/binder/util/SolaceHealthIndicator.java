package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.concurrent.atomic.AtomicBoolean;

public class SolaceHealthIndicator extends AbstractHealthIndicator implements SessionEventHandler {

    private final AtomicBoolean connected = new AtomicBoolean(false);

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        if(connected.get()) {
            builder.up();
        } else {
            builder.down();
        }
    }

    @Override
    public void handleEvent(SessionEventArgs sessionEventArgs) {
        switch (sessionEventArgs.getEvent()) {
            case DOWN_ERROR:
                connected.set(false);
                break;
            case RECONNECTED:
                connected.set(true);
                break;
            case RECONNECTING:
            case SUBSCRIPTION_ERROR:
            case UNKNOWN_TRANSACTED_SESSION_NAME:
            case VIRTUAL_ROUTER_NAME_CHANGED:
            default:
                break;
        }
    }

    public void connected() {
        this.connected.set(true);
    }
}
