package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.InvalidOperationException;
import com.solacesystems.jcsmp.JCSMPErrorResponseException;
import com.solacesystems.jcsmp.JCSMPErrorResponseSubcodeEx;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.TopicEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.cloud.stream.provisioning.ProvisioningProvider;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SolaceEndpointProvisioner
		implements ProvisioningProvider<ExtendedConsumerProperties<SolaceConsumerProperties>,ExtendedProducerProperties<SolaceProducerProperties>> {

	private final JCSMPSession jcsmpSession;

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceEndpointProvisioner.class);

	public SolaceEndpointProvisioner(JCSMPSession jcsmpSession) {
		this.jcsmpSession = jcsmpSession;
	}

	@Override
	public ProducerDestination provisionProducerDestination(String name,
															ExtendedProducerProperties<SolaceProducerProperties> properties)
			throws ProvisioningException {

		if (properties.isPartitioned()) {
			LOGGER.warn("Partitioning is not supported with this version of Solace's cloud stream binder.\n" +
					"Provisioning will continue under the assumption that it is disabled...");
		}

		switch (properties.getExtension().getDestinationType()) {
			case QUEUE -> {
				if (properties.getRequiredGroups() != null && properties.getRequiredGroups().length > 0) {
					throw new ProvisioningException(String.format("Producer requiredGroups are not supported when destinationType=%s", DestinationType.QUEUE));
				}
				provisionQueueIfRequired(name, properties);
				return new SolaceProducerDestination(name);
			}
			case TOPIC -> {
				String topicName = SolaceProvisioningUtil.getTopicName(name, properties.getExtension());

				Set<String> requiredGroups = new HashSet<>(Arrays.asList(properties.getRequiredGroups()));
				Map<String,String[]> requiredGroupsExtraSubs = properties.getExtension().getQueueAdditionalSubscriptions();

				for (String groupName : requiredGroups) {
					String queueName = SolaceProvisioningUtil.getQueueName(topicName, groupName, properties);
					LOGGER.info("Creating durable endpoint {} for required consumer group {}", queueName, groupName);
					Queue queue = provisionQueueIfRequired(queueName, properties);
					addSubscriptionToQueue(queue, topicName, properties.getExtension(), true);

					for (String extraTopic : requiredGroupsExtraSubs.getOrDefault(groupName, new String[0])) {
						addSubscriptionToQueue(queue, extraTopic, properties.getExtension(), false);
					}
				}

				Set<String> ignoredExtraSubs = requiredGroupsExtraSubs.keySet()
						.stream()
						.filter(g -> !requiredGroups.contains(g))
						.collect(Collectors.toSet());

				if (ignoredExtraSubs.size() > 0) {
					LOGGER.warn("Groups [{}] are not required groups. The additional subscriptions defined for them were ignored...",
							String.join(", ", ignoredExtraSubs));
				}

				return new SolaceProducerDestination(topicName);
			}
			default -> throw new ProvisioningException(String.format("Destination type %s is not supported for producers",
					properties.getExtension().getDestinationType()));
		}
	}

	@Override
	public ConsumerDestination provisionConsumerDestination(String name, String group,
															ExtendedConsumerProperties<SolaceConsumerProperties> properties)
			throws ProvisioningException {

		if (properties.isPartitioned()) {
			LOGGER.warn("Partitioning is not supported with this version of Solace's cloud stream binder.\n" +
					"Provisioning will continue under the assumption that it is disabled...");
		}

		boolean isAnonEndpoint = SolaceProvisioningUtil.isAnonEndpoint(group);
		boolean isDurableEndpoint = SolaceProvisioningUtil.isDurableEndpoint(group);
		SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil.getQueueNames(name, group, properties, isAnonEndpoint);
		String groupQueueName = queueNames.getConsumerGroupQueueName();

		EndpointProperties endpointProperties = SolaceProvisioningUtil.getEndpointProperties(properties.getExtension());
		ConsumerFlowProperties consumerFlowProperties = SolaceProvisioningUtil.getConsumerFlowProperties(name, properties);

		EndpointType endpointType = properties.getExtension().getEndpointType();
		if (EndpointType.TOPIC_ENDPOINT.equals(endpointType)) {
			consumerFlowProperties.setNewSubscription(JCSMPFactory.onlyInstance().createTopic(name));
		}

		EndpointProvider<?> endpointProvider = EndpointProvider.from(endpointType);

		if (properties.getConcurrency() > 1) {
			if (endpointProperties.getAccessType().equals(EndpointProperties.ACCESSTYPE_EXCLUSIVE)) {
				String msg = "Concurrency > 1 is not supported when using exclusive queues, " +
						"either configure a concurrency of 1 or use a non-exclusive queue";
				LOGGER.warn(msg);
				throw new ProvisioningException(msg);
			} else if (!StringUtils.hasText(group)) {
				String msg = "Concurrency > 1 is not supported when using anonymous consumer groups, " +
						"either configure a concurrency of 1 or define a consumer group";
				LOGGER.warn(msg);
				throw new ProvisioningException(msg);
			}
		}

		LOGGER.info(isAnonEndpoint ?
				String.format("Creating anonymous (temporary) %s %s", endpointType, groupQueueName) :
				String.format("Creating %s %s %s for consumer group %s",
						isDurableEndpoint ? "durable" : "temporary", endpointType, groupQueueName, group));
		Endpoint endpoint = provisionEndpoint(groupQueueName, endpointProvider, isDurableEndpoint, endpointProperties,
				consumerFlowProperties, properties.getExtension().isProvisionDurableQueue(), properties.isAutoStartup());

		Set<String> additionalSubscriptions = EndpointType.QUEUE.equals(endpointType) ?
				Set.of(properties.getExtension().getQueueAdditionalSubscriptions()) :
				Collections.emptySet();

		String errorQueueName = null;
		if (properties.getExtension().isAutoBindErrorQueue()) {
			errorQueueName = provisionErrorQueue(queueNames.getErrorQueueName(), properties).getName();
		}

		return new SolaceConsumerDestination(endpoint.getName(), name, queueNames.getPhysicalGroupName(), !isDurableEndpoint,
				errorQueueName, additionalSubscriptions);
	}

	private Queue provisionQueueIfRequired(String queueName, ExtendedProducerProperties<SolaceProducerProperties> properties) {
		EndpointProperties endpointProperties = SolaceProvisioningUtil.getEndpointProperties(properties.getExtension());
		boolean doDurableQueueProvisioning = properties.getExtension().isProvisionDurableQueue();
		return provisionEndpoint(queueName, EndpointProvider.QUEUE_PROVIDER, true, endpointProperties,
				new ConsumerFlowProperties(), doDurableQueueProvisioning, properties.isAutoStartup());
	}

	private <T extends Endpoint> T provisionEndpoint(
			String name,
			EndpointProvider<T> endpointProvider,
			boolean isDurable,
			EndpointProperties endpointProperties,
			ConsumerFlowProperties consumerFlowProperties,
			boolean doDurableProvisioning,
			boolean testFlowCxn) throws ProvisioningException {

		T endpoint;
		try {
			if (isDurable) {
				endpoint = endpointProvider.createInstance(name);
				if (doDurableProvisioning) {
					jcsmpSession.provision(endpoint, endpointProperties, JCSMPSession.FLAG_IGNORE_ALREADY_EXISTS);
				} else {
					LOGGER.debug("Provisioning is disabled, {} will not be provisioned nor will its configuration be validated",
							name);
				}
			} else {
				// EndpointProperties will be applied during consumer creation
				endpoint = endpointProvider.createTemporaryEndpoint(name, jcsmpSession);
			}
		} catch (Exception e) {
			String action = isDurable ? "provision durable" : "create temporary";
			String msg = String.format("Failed to %s endpoint %s", action, name);
			LOGGER.warn(msg, e);
			throw new ProvisioningException(msg, e);
		}

		if (testFlowCxn) {
			testFlowConnection(endpoint, endpointProperties, consumerFlowProperties, doDurableProvisioning);
		} else {
			LOGGER.trace("Skipping test consumer flow connection for {} {}", endpoint.getClass().getSimpleName(), name);
		}

		return endpoint;
	}

	private void testFlowConnection(Endpoint endpoint,
									EndpointProperties endpointProperties,
									ConsumerFlowProperties consumerFlowProperties,
									boolean wasDurableProvisioned) {
		String endpointType = getEndpointTypeLabel(endpoint);

		try {
			LOGGER.info("Testing consumer flow connection to {} {} (will not start it)",
					endpointType, endpoint.getName());
			final ConsumerFlowProperties testFlowProperties = consumerFlowProperties.setEndpoint(endpoint)
					.setStartState(false);
			jcsmpSession.createFlow(null, testFlowProperties, endpointProperties).close();
			LOGGER.info("Connected test consumer flow to {} {}, closing it",
					endpointType, endpoint.getName());
		} catch (JCSMPException e) {
			String msg = String.format("Failed to connect test consumer flow to %s %s",
					endpointType, endpoint.getName());

			if (endpoint.isDurable() && !wasDurableProvisioned) {
				msg += ". Provisioning is disabled, " + endpointType +
						" was not provisioned nor was its configuration validated.";
			}

			if (e instanceof InvalidOperationException && !endpoint.isDurable()) {
				msg += ". If the Solace client is not capable of creating temporary " + endpointType +
						", consider assigning this consumer to a group?";
			}
			LOGGER.warn(msg, e);
			throw new ProvisioningException(msg, e);
		}
	}

	private Queue provisionErrorQueue(String errorQueueName, ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		LOGGER.info("Provisioning error queue {}", errorQueueName);
		EndpointProperties endpointProperties = SolaceProvisioningUtil.getErrorQueueEndpointProperties(properties.getExtension());
		return provisionEndpoint(errorQueueName,
				EndpointProvider.QUEUE_PROVIDER,
				true,
				endpointProperties,
				new ConsumerFlowProperties(),
				properties.getExtension().isProvisionErrorQueue(),
				properties.isAutoStartup());
	}

	public void addSubscriptionToQueue(Queue queue, String topicName, SolaceCommonProperties properties, boolean isDestinationSubscription) {
		if (!isDestinationSubscription && queue.isDurable() && !properties.isAddDestinationAsSubscriptionToQueue()) {
			LOGGER.debug("Provision subscriptions to durable queues was disabled, queue {} will not be subscribed to topic {}",
					queue.getName(), topicName);
			return;
		}

		if (isDestinationSubscription && !properties.isAddDestinationAsSubscriptionToQueue()) {
			LOGGER.debug("Adding destination as subscription was disabled, queue {} will not be subscribed to topic {}",
					queue.getName(), topicName);
			return;
		}

		LOGGER.info("Subscribing queue {} to topic {}", queue.getName(), topicName);
		try {
			Topic topic = JCSMPFactory.onlyInstance().createTopic(topicName);
			try {
				jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);
			} catch (JCSMPErrorResponseException e) {
				if (e.getSubcodeEx() == JCSMPErrorResponseSubcodeEx.SUBSCRIPTION_ALREADY_PRESENT) {
					LOGGER.info("Queue {} is already subscribed to topic {}, SUBSCRIPTION_ALREADY_PRESENT error will be ignored...",
							queue.getName(), topicName);
				} else {
					throw e;
				}
			}
		} catch (JCSMPException e) {
			String msg = String.format("Failed to add subscription of %s to queue %s", topicName, queue.getName());
			LOGGER.warn(msg, e);
			throw new ProvisioningException(msg, e);
		}
	}

	private String getEndpointTypeLabel(Endpoint endpoint) {
		return endpoint instanceof TopicEndpoint ? "topic endpoint" : "queue";
	}
}
