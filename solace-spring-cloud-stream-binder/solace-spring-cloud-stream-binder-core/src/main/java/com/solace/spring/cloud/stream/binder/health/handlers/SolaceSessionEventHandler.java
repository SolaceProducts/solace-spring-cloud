package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SessionEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolaceSessionEventHandler implements SessionEventHandler {
	private final SessionHealthIndicator sessionHealthIndicator;
	private static final Log logger = LogFactory.getLog(SolaceSessionEventHandler.class);

	public SolaceSessionEventHandler(SessionHealthIndicator sessionHealthIndicator) {
		this.sessionHealthIndicator = sessionHealthIndicator;
	}

	@Override
	public void handleEvent(SessionEventArgs eventArgs) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Received Solace JCSMP Session event [%s]", eventArgs));
		}
		switch (eventArgs.getEvent()) {
			case RECONNECTED -> this.sessionHealthIndicator.up();
			case DOWN_ERROR -> this.sessionHealthIndicator.down(eventArgs);
			case RECONNECTING -> this.sessionHealthIndicator.reconnecting(eventArgs);
		}
	}

	public void setSessionHealthUp() {
		this.sessionHealthIndicator.up();
	}
}
