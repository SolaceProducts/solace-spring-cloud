package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import org.springframework.util.StringUtils;

import java.util.UUID;

public class SolaceProvisioningUtil {
	private static final String QUEUE_NAME_DELIM = "/";
	private static final String QUEUE_NAME_SEGMENT_ERROR = "error";

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
		return baseTopicName;
	}

	public static String getQueueName(String topicName, String groupName,
									  SolaceProducerProperties producerProperties) {
		return getQueueNames(topicName, groupName, producerProperties, false, true, true)
				.getConsumerGroupQueueName();
	}

	public static QueueNames getQueueNames(String topicName, String groupName,
										   SolaceConsumerProperties consumerProperties, boolean isAnonymous) {
		QueueNames queueNames = getQueueNames(topicName, groupName, consumerProperties,
				isAnonymous,
				consumerProperties.isUseGroupNameInQueueName(),
				consumerProperties.isUseGroupNameInErrorQueueName());

		if (StringUtils.hasText(consumerProperties.getErrorQueueNameOverride())) {
			return new QueueNames(queueNames.getConsumerGroupQueueName(),
					consumerProperties.getErrorQueueNameOverride(), queueNames.getPhysicalGroupName());
		} else {
			return queueNames;
		}
	}

	private static QueueNames getQueueNames(String topicName, String groupName, SolaceCommonProperties properties,
											boolean isAnonymous,
											boolean useGroupName, boolean useGroupNameInErrorQueue) {
		final String physicalGroupName = isAnonymous ? UUID.randomUUID().toString() : groupName;
		StringBuilder groupQueueName = new StringBuilder();
		StringBuilder errorQueueName = new StringBuilder();

		if (StringUtils.hasText(properties.getQueueNamePrefix())) {
			groupQueueName.append(properties.getQueueNamePrefix()).append(QUEUE_NAME_DELIM);
			errorQueueName.append(properties.getQueueNamePrefix()).append(QUEUE_NAME_DELIM);
		}

		errorQueueName.append(QUEUE_NAME_SEGMENT_ERROR).append(QUEUE_NAME_DELIM);

		if (isAnonymous) {
			groupQueueName.append(QueueNameFamiliarity.ANON.getLabel())
					.append(QUEUE_NAME_DELIM)
					.append(physicalGroupName)
					.append(QUEUE_NAME_DELIM);
			errorQueueName.append(QueueNameFamiliarity.ANON.getLabel())
					.append(QUEUE_NAME_DELIM)
					.append(physicalGroupName)
					.append(QUEUE_NAME_DELIM);
		} else {
			groupQueueName.append(QueueNameFamiliarity.WELL_KNOWN.getLabel()).append(QUEUE_NAME_DELIM);
			errorQueueName.append(QueueNameFamiliarity.WELL_KNOWN.getLabel()).append(QUEUE_NAME_DELIM);
			if (useGroupName) {
				groupQueueName.append(physicalGroupName).append(QUEUE_NAME_DELIM);
			}
			if (useGroupNameInErrorQueue) {
				errorQueueName.append(physicalGroupName).append(QUEUE_NAME_DELIM);
			}
		}

		String encodedDestination = replaceTopicWildCards(topicName, "_");
		groupQueueName.append(QueueNameDestinationEncoding.PLAIN.getLabel())
				.append(QUEUE_NAME_DELIM)
				.append(encodedDestination);
		errorQueueName.append(QueueNameDestinationEncoding.PLAIN.getLabel())
				.append(QUEUE_NAME_DELIM)
				.append(encodedDestination);

		return new QueueNames(groupQueueName.toString(), errorQueueName.toString(), physicalGroupName);
	}

	private static String replaceTopicWildCards(String topicName, CharSequence replacement) {
		return topicName
				.replace("*", replacement)
				.replace(">", replacement);
	}

	public static class QueueNames {
		private final String consumerGroupQueueName;
		private final String errorQueueName;
		private final String physicalGroupName;

		private QueueNames(String consumerGroupQueueName, String errorQueueName, String physicalGroupName) {
			this.consumerGroupQueueName = consumerGroupQueueName;
			this.errorQueueName = errorQueueName;
			this.physicalGroupName = physicalGroupName;
		}

		public String getConsumerGroupQueueName() {
			return consumerGroupQueueName;
		}

		public String getErrorQueueName() {
			return errorQueueName;
		}

		public String getPhysicalGroupName() {
			return physicalGroupName;
		}
	}

	private enum QueueNameFamiliarity {
		WELL_KNOWN("wk"), ANON("an");
		private final String label;

		QueueNameFamiliarity(String label) {
			this.label = label;
		}

		public String getLabel() {
			return label;
		}
	}
}
