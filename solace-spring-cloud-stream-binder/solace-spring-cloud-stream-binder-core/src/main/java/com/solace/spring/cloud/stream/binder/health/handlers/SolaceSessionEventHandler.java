package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solacesystems.jcsmp.DefaultSolaceOAuth2SessionEventHandler;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.SolaceSessionOAuth2TokenProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolaceSessionEventHandler extends DefaultSolaceOAuth2SessionEventHandler {
	private final SessionHealthIndicator sessionHealthIndicator;
	private static final Log logger = LogFactory.getLog(SolaceSessionEventHandler.class);

	public SolaceSessionEventHandler(JCSMPProperties jcsmpProperties,
			SolaceSessionOAuth2TokenProvider solaceSessionOAuth2TokenProvider,
			SessionHealthIndicator sessionHealthIndicator) {
		super(jcsmpProperties, solaceSessionOAuth2TokenProvider);
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
			case RECONNECTING -> {
				super.handleEvent(eventArgs);
				this.sessionHealthIndicator.reconnecting(eventArgs);
			}
		}
	}

	public void setSessionHealthUp() {
		this.sessionHealthIndicator.up();
	}
}
