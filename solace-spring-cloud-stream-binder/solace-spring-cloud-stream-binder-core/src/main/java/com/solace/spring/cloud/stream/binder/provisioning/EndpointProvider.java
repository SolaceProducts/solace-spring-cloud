package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.TopicEndpoint;

public interface EndpointProvider<T extends Endpoint> {
	QueueProvider QUEUE_PROVIDER = new QueueProvider();
	TopicEndpointProvider TOPIC_ENDPOINT_PROVIDER = new TopicEndpointProvider();

	T createInstance(String name);
	T createTemporaryEndpoint(String name, JCSMPSession jcsmpSession) throws JCSMPException;

	static EndpointProvider<?> from(EndpointType endpointType) {
		return switch (endpointType) {
			case QUEUE -> QUEUE_PROVIDER;
			case TOPIC_ENDPOINT -> TOPIC_ENDPOINT_PROVIDER;
		};
	}

	class QueueProvider implements EndpointProvider<Queue> {
		@Override
		public Queue createInstance(String name) {
			return JCSMPFactory.onlyInstance().createQueue(name);
		}

		@Override
		public Queue createTemporaryEndpoint(String name, JCSMPSession jcsmpSession) throws JCSMPException {
			return jcsmpSession.createTemporaryQueue();
		}
	}

	class TopicEndpointProvider implements EndpointProvider<TopicEndpoint> {
		@Override
		public TopicEndpoint createInstance(String name) {
			return JCSMPFactory.onlyInstance().createDurableTopicEndpointEx(name);
		}

		@Override
		public TopicEndpoint createTemporaryEndpoint(String name, JCSMPSession jcsmpSession) throws JCSMPException {
			return jcsmpSession.createNonDurableTopicEndpoint(name);
		}
	}
}
