package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import org.springframework.util.StringUtils;

public class SolaceProvisioningUtil {
	private static final String QUEUE_NAME_DELIM = ".";
	private static final String ERROR_QUEUE_POSTFIX = "error";

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

	public static EndpointProperties getErrorQueueEndpointProperties(SolaceConsumerProperties properties) {
		EndpointProperties endpointProperties = new EndpointProperties();
		endpointProperties.setAccessType(properties.getErrorQueueAccessType());
		endpointProperties.setDiscardBehavior(properties.getErrorQueueDiscardBehaviour());
		endpointProperties.setMaxMsgRedelivery(properties.getErrorQueueMaxMsgRedelivery());
		endpointProperties.setMaxMsgSize(properties.getErrorQueueMaxMsgSize());
		endpointProperties.setPermission(properties.getErrorQueuePermission());
		endpointProperties.setQuota(properties.getErrorQueueQuota());
		endpointProperties.setRespectsMsgTTL(properties.getErrorQueueRespectsMsgTtl());
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

	public static String getErrorQueueName(String queueName) {
		return queueName + QUEUE_NAME_DELIM + ERROR_QUEUE_POSTFIX;
	}
}
