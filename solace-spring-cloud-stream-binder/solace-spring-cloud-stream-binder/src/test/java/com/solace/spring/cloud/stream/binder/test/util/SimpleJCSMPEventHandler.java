package com.solace.spring.cloud.stream.binder.test.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleJCSMPEventHandler implements JCSMPStreamingPublishCorrelatingEventHandler {
	private static final Logger logger = LoggerFactory.getLogger(SimpleJCSMPEventHandler.class);

	private final boolean logErrors;

	public SimpleJCSMPEventHandler() {
		this(true);
	}

	public SimpleJCSMPEventHandler(boolean logErrors) {
		this.logErrors = logErrors;
	}

	@Override
	public void responseReceivedEx(Object key) {
		logger.debug("Got message with key: {}", key);
	}

	@Override
	public void handleErrorEx(Object key, JCSMPException e, long timestamp) {
		if (logErrors) {
			logger.error("Failed to receive message at {} with key {}", timestamp, key, e);
		} else {
			logger.trace("Failed to receive message at {} with key {}", timestamp, key, e);
		}
	}
}
