package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import org.springframework.util.StringUtils;

public class SolaceProvisioningUtil {
	private static final String QUEUE_NAME_DELIM = ".";
	private static final String DMQ_POSTFIX = "dmq";

	private SolaceProvisioningUtil() {}

	public static EndpointProperties getEndpointProperties(SolaceCommonProperties properties) {
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(properties.getQueueAccessType());
		endpointProperties.setDiscardBehavior(properties.getQueueDiscardBehaviour());
		endpointProperties.setMaxMsgRedelivery(properties.getQueueMaxMsgRedelivery());
		endpointProperties.setMaxMsgSize(properties.getQueueMaxMsgSize());
		endpointProperties.setPermission(properties.getQueuePermission());
		endpointProperties.setQuota(properties.getQueueQuota());
		endpointProperties.setRespectsMsgTTL(properties.getQueueRespectsMsgTtl());
		return endpointProperties;
	}

	public static EndpointProperties getDMQEndpointProperties(SolaceConsumerProperties properties) {
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(properties.getDmqAccessType());
		endpointProperties.setDiscardBehavior(properties.getDmqDiscardBehaviour());
		endpointProperties.setMaxMsgRedelivery(properties.getDmqMaxMsgRedelivery());
		endpointProperties.setMaxMsgSize(properties.getDmqMaxMsgSize());
		endpointProperties.setPermission(properties.getDmqPermission());
		endpointProperties.setQuota(properties.getDmqQuota());
		endpointProperties.setRespectsMsgTTL(properties.getDmqRespectsMsgTtl());
		return endpointProperties;
	}

	public static boolean isAnonQueue(String groupName) {
		return !StringUtils.hasText(groupName);
	}

	public static boolean isDurableQueue(String groupName) {
		return !isAnonQueue(groupName);
	}

	public static String getTopicName(String baseTopicName, SolaceCommonProperties properties) {
		return properties.getPrefix() + baseTopicName;
	}

	public static String getQueueName(String topicName, String groupName,
									  SolaceProducerProperties producerProperties) {
		return getQueueName(topicName, groupName, producerProperties,
				false, null);
	}

	public static String getQueueName(String topicName, String groupName,
								SolaceConsumerProperties consumerProperties, boolean isAnonymous) {
		return getQueueName(topicName, groupName, consumerProperties,
				isAnonymous, consumerProperties.getAnonymousGroupPostfix());
	}

	private static String getQueueName(String topicName, String groupName,
								SolaceCommonProperties properties,
								boolean isAnonymous, String anonGroupPostfix) {
		String queueName;
		if (isAnonymous) {
			queueName = topicName + QUEUE_NAME_DELIM + JCSMPFactory.onlyInstance().createUniqueName(anonGroupPostfix);
		} else {
			queueName = topicName + QUEUE_NAME_DELIM + groupName;
		}

		return properties.getPrefix() + queueName;
	}

	public static String getDMQName(String queueName) {
		return queueName + QUEUE_NAME_DELIM + DMQ_POSTFIX;
	}
}
