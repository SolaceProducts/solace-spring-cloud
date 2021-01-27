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
		return getQueueNames(topicName, groupName, producerProperties, false, null, true, true)
				.getConsumerGroupQueueName();
	}

	public static QueueNames getQueueNames(String topicName, String groupName,
										   SolaceConsumerProperties consumerProperties, boolean isAnonymous) {
		return getQueueNames(topicName, groupName, consumerProperties,
				isAnonymous, consumerProperties.getAnonymousGroupPostfix(),
				consumerProperties.isUseGroupNameInQueueName(),
				consumerProperties.isUseGroupNameInErrorQueueName());
	}

	private static QueueNames getQueueNames(String topicName, String groupName, SolaceCommonProperties properties,
											boolean isAnonymous, String anonGroupPostfix,
											boolean useGroupName, boolean useGroupNameInErrorQueue) {
		String commonPrefix = properties.getPrefix() + replaceTopicWildCards(topicName, "_");
		StringBuilder groupQueueName = new StringBuilder(commonPrefix);
		StringBuilder errorQueueName = new StringBuilder(commonPrefix);

		if (isAnonymous) {
			String uniquePostfix = JCSMPFactory.onlyInstance().createUniqueName(anonGroupPostfix);
			groupQueueName.append(QUEUE_NAME_DELIM).append(uniquePostfix);
			errorQueueName.append(QUEUE_NAME_DELIM).append(uniquePostfix);
		} else {
			if (useGroupName) {
				groupQueueName.append(QUEUE_NAME_DELIM).append(groupName);
			}
			if (useGroupNameInErrorQueue) {
				errorQueueName.append(QUEUE_NAME_DELIM).append(groupName);
			}
		}

		errorQueueName.append(QUEUE_NAME_DELIM).append(ERROR_QUEUE_POSTFIX);

		return new QueueNames(groupQueueName.toString(), errorQueueName.toString());
	}

	private static String replaceTopicWildCards(String topicName, CharSequence replacement) {
		return topicName
				.replace("*", replacement)
				.replace(">", replacement);
	}

	public static class QueueNames {
		private final String consumerGroupQueueName;
		private final String errorQueueName;

		private QueueNames(String consumerGroupQueueName, String errorQueueName) {
			this.consumerGroupQueueName = consumerGroupQueueName;
			this.errorQueueName = errorQueueName;
		}

		public String getConsumerGroupQueueName() {
			return consumerGroupQueueName;
		}

		public String getErrorQueueName() {
			return errorQueueName;
		}
	}
}
