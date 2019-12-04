package com.solace.spring.cloud.stream.binder;

import com.solacesystems.jcsmp.InvalidPropertiesException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Assume;

class SolaceExternalResourceHandler {

	private JCSMPException sessionConnectError;

	private static final Log logger = LogFactory.getLog(SolaceExternalResourceHandler.class);

	JCSMPSession assumeAndGetActiveSession(SpringJCSMPFactory springJCSMPFactory, boolean failOnConnectionException)
			throws InvalidPropertiesException {

		handleConnectionError(failOnConnectionException);
		JCSMPSession session = springJCSMPFactory.createSession();

		try {
			session.connect();
		} catch (JCSMPException e) {
			sessionConnectError = e;
			handleConnectionError(failOnConnectionException);
		}

		return session;
	}

	private void handleConnectionError(boolean failOnConnectionException) {
		if (sessionConnectError == null) return;

		String connectMsg = "Failed to establish connection to a Solace message broker. Skipping test...";
		logger.warn(connectMsg);
		if (failOnConnectionException) {
			Assert.fail(connectMsg);
		}
		else {
			Assume.assumeNoException(connectMsg, sessionConnectError);
		}
	}
}
