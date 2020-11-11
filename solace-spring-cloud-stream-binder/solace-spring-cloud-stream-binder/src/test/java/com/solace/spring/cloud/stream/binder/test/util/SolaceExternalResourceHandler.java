package com.solace.spring.cloud.stream.binder.test.util;

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.junit.Assert;

public class SolaceExternalResourceHandler {

	private JCSMPException sessionConnectError;

	public JCSMPSession assumeAndGetActiveSession(SpringJCSMPFactory springJCSMPFactory)
			throws InvalidPropertiesException {

		handleConnectionError();
		JCSMPSession session = springJCSMPFactory.createSession();

		try {
			session.connect();
		} catch (JCSMPException e) {
			sessionConnectError = e;
			handleConnectionError();
		}

		return session;
	}

	private void handleConnectionError() {
		if (sessionConnectError != null) {
			Assert.fail("Failed to establish connection to a Solace message broker.");
		}
	}
}
