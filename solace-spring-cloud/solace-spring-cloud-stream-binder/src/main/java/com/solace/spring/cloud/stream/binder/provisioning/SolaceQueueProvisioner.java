package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.util.SolaceProvisioningUtil;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SolaceQueueProvisioner
		implements ProvisioningProvider<ExtendedConsumerProperties<SolaceConsumerProperties>,ExtendedProducerProperties<SolaceProducerProperties>> {

	private JCSMPSession jcsmpSession;
	private Map<String, Set<String>> queueToTopicBindings = new HashMap<>();

	private static final Log logger = LogFactory.getLog(SolaceQueueProvisioner.class);

	public SolaceQueueProvisioner(JCSMPSession jcsmpSession) {
		this.jcsmpSession = jcsmpSession;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
															ExtendedProducerProperties<SolaceProducerProperties> properties)
			throws ProvisioningException {

		if (properties.isPartitioned()) {
			logger.warn("Partitioning is not supported with this version of Solace's cloud stream binder.\n" +
					"Provisioning will continue under the assumption that it is disabled...");
		}

		String topicName = SolaceProvisioningUtil.getTopicName(name, properties.getExtension());

		Set<String> requiredGroups = new HashSet<>(Arrays.asList(properties.getRequiredGroups()));
		Map<String,String[]> requiredGroupsExtraSubs = properties.getExtension().getQueueAdditionalSubscriptions();

		for (String groupName : requiredGroups) {
			String queueName = SolaceProvisioningUtil.getQueueName(name, groupName, properties.getExtension());
			EndpointProperties endpointProperties = SolaceProvisioningUtil.getEndpointProperties(properties.getExtension());
			logger.info(String.format("Creating durable queue %s for required consumer group %s", queueName, groupName));
			Queue queue = provisionQueue(queueName, true, endpointProperties);

			addSubscriptionToQueue(queue, topicName);
			trackQueueToTopicBinding(queue.getName(), topicName);

			for (String extraTopic : requiredGroupsExtraSubs.getOrDefault(groupName, new String[0])) {
				addSubscriptionToQueue(queue, extraTopic);
				trackQueueToTopicBinding(queue.getName(), extraTopic);
			}
		}

		Set<String> ignoredExtraSubs = requiredGroupsExtraSubs.keySet()
				.stream()
				.filter(g -> !requiredGroups.contains(g))
				.collect(Collectors.toSet());

		if (ignoredExtraSubs.size() > 0) {
			logger.warn(String.format(
					"Groups [%s] are not required groups. The additional subscriptions defined for them were ignored...",
					String.join(", ", ignoredExtraSubs)));
		}

		return new SolaceProducerDestination(topicName);
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
															ExtendedConsumerProperties<SolaceConsumerProperties> properties)
			throws ProvisioningException {

		if (properties.isPartitioned()) {
			logger.warn("Partitioning is not supported with this version of Solace's cloud stream binder.\n" +
					"Provisioning will continue under the assumption that it is disabled...");
		}

		String topicName = SolaceProvisioningUtil.getTopicName(name, properties.getExtension());
		boolean isAnonQueue = SolaceProvisioningUtil.isAnonQueue(group);
		boolean isDurableQueue = SolaceProvisioningUtil.isDurableQueue(group);
		String queueName = SolaceProvisioningUtil.getQueueName(name, group, properties.getExtension(), isAnonQueue);

		logger.info(isAnonQueue ?
				String.format("Creating anonymous (temporary) queue %s", queueName) :
				String.format("Creating %s queue %s for consumer group %s", isDurableQueue ? "durable" : "temporary", queueName, group));
		EndpointProperties endpointProperties = SolaceProvisioningUtil.getEndpointProperties(properties.getExtension());
		Queue queue = provisionQueue(queueName, isDurableQueue, endpointProperties);
		trackQueueToTopicBinding(queue.getName(), topicName);

		for (String additionalSubscription : properties.getExtension().getQueueAdditionalSubscriptions()) {
			trackQueueToTopicBinding(queue.getName(), additionalSubscription);
		}

		if (properties.getExtension().isAutoBindDmq()) {
			provisionDMQ(queueName, properties.getExtension());
		}

		return new SolaceConsumerDestination(queue.getName());
	}

	private Queue provisionQueue(String name, boolean isDurable, EndpointProperties endpointProperties) throws ProvisioningException {
		Queue queue;
		if (isDurable) {
			try {
				queue = JCSMPFactory.onlyInstance().createQueue(name);
				jcsmpSession.provision(queue, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
			} catch (JCSMPException e) {
				String msg = String.format("Failed to provision durable queue %s", name);
				logger.warn(msg, e);
				throw new ProvisioningException(msg, e);
			}
		} else {
			try {
				// EndpointProperties will be applied during consumer creation
				queue = jcsmpSession.createTemporaryQueue(name);
			} catch (JCSMPException e) {
				String msg = String.format("Failed to create temporary queue %s", name);
				logger.warn(msg, e);
				throw new ProvisioningException(msg, e);
			}
		}

		return queue;
	}

	private void provisionDMQ(String queueName, SolaceConsumerProperties properties) {
		String dmqName = SolaceProvisioningUtil.getDMQName(queueName);
		logger.info(String.format("Provisioning DMQ %s", dmqName));
		provisionQueue(dmqName, true, SolaceProvisioningUtil.getDMQEndpointProperties(properties));
	}

	public void addSubscriptionToQueue(Queue queue, String topicName) {
		logger.info(String.format("Subscribing queue %s to topic %s", queue.getName(), topicName));
		try {
			Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
			try {
				jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);
			} catch (JCSMPErrorResponseException e) {
				if (e.getSubcodeEx() == JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
					logger.warn(String.format(
							"Queue %s is already subscribed to topic %s, SUBSCRIPTION_ALREADY_PRESENT error will be ignored...",
							queue.getName(), topicName));
				} else {
					throw e;
				}
			}
		} catch (JCSMPException e) {
			String msg = String.format("Failed to add subscription of %s to queue %s", topicName, queue.getName());
			logger.warn(msg, e);
			throw new ProvisioningException(msg, e);
		}
	}

	public Set<String> getTrackedTopicsForQueue(String queueName) {
		return queueToTopicBindings.get(queueName);
	}

	private void trackQueueToTopicBinding(String queueName, String topicName) {
		if (! queueToTopicBindings.containsKey(queueName)) {
			queueToTopicBindings.put(queueName, new HashSet<>());
		}
		queueToTopicBindings.get(queueName).add(topicName);
	}
}
