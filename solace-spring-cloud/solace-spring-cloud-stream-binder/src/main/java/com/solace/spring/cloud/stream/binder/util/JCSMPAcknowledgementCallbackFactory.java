package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.XMLMessage;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.acks.AcknowledgmentCallbackFactory;

class JCSMPAcknowledgementCallbackFactory implements AcknowledgmentCallbackFactory<XMLMessage> {

	@Override
	public AcknowledgmentCallback createCallback(XMLMessage xmlMessage) {
		return new JCSMPAcknowledgementCallback(xmlMessage);
	}

	static class JCSMPAcknowledgementCallback implements AcknowledgmentCallback {
		private XMLMessage xmlMessage;
		private boolean acknowledged = false;
		private boolean autoAckEnabled = true;

		JCSMPAcknowledgementCallback(XMLMessage xmlMessage) {
			this.xmlMessage = xmlMessage;
		}

		@Override
		public void acknowledge(Status status) {
			switch (status) {
				case ACCEPT:
					xmlMessage.ackMessage();
					break;
				case REJECT:
					xmlMessage.ackMessage();
					break;
				case REQUEUE:
					xmlMessage.ackMessage();
			}

			acknowledged = true;
		}

		@Override
		public boolean isAcknowledged() {
			return acknowledged;
		}

		@Override
		public void noAutoAck() {
			autoAckEnabled = false;
		}

		@Override
		public boolean isAutoAck() {
			return autoAckEnabled;
		}
	}
}
