package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JCSMPSessionProducerManager extends SharedResourceManager<XMLMessageProducer> {
	private JCSMPSession session;

	private static final Log logger = LogFactory.getLog(JCSMPSessionProducerManager.class);

	public JCSMPSessionProducerManager(JCSMPSession session) {
		super("producer");
		this.session = session;
	}

	@Override
	XMLMessageProducer create() throws JCSMPException {
		return session.getMessageProducer(publisherEventHandler);
	}

	@Override
	void close() {
		sharedResource.close();
	}

	private JCSMPStreamingPublishEventHandler publisherEventHandler = new JCSMPStreamingPublishEventHandler() {
		@Override
		public void responseReceived(String messageID) {
			logger.debug("Producer received response for msg: " + messageID);
		}

		@Override
		public void handleError(String messageID, JCSMPException e, long timestamp) {
			logger.warn("Producer received error for msg: " + messageID + " - " + timestamp, e);
		}
	};
}
