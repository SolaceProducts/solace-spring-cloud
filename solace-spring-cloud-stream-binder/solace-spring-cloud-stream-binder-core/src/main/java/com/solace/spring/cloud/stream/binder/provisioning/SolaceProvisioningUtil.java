package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.ProducerFlowProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionException;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

import java.util.UUID;

public class SolaceProvisioningUtil {

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

	public static ProducerFlowProperties getProducerFlowProperties(JCSMPSession jcsmpSession) {
		ProducerFlowProperties producerFlowProperties = new ProducerFlowProperties();

		// SOL-118898:
		// PUB_ACK_WINDOW_SIZE & ACK_EVENT_MODE aren't automatically used as default values for
		// ProducerFlowProperties.
		Integer pubAckWindowSize = (Integer) jcsmpSession.getProperty(JCSMPProperties.PUB_ACK_WINDOW_SIZE);
		if (pubAckWindowSize != null) {
			producerFlowProperties.setWindowSize(pubAckWindowSize);
		}
		String ackEventMode = (String) jcsmpSession.getProperty(JCSMPProperties.ACK_EVENT_MODE);
		if (ackEventMode != null) {
			producerFlowProperties.setAckEventMode(ackEventMode);
		}

		return producerFlowProperties;
	}

	public static ConsumerFlowProperties getConsumerFlowProperties(
			String destinationName,
			ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		ConsumerFlowProperties consumerFlowProperties = new ConsumerFlowProperties();
		final String selector = properties.getExtension().getSelector();
		consumerFlowProperties.setSelector((selector == null || selector.isBlank()) ? null : selector);

		if (EndpointType.TOPIC_ENDPOINT.equals(properties.getExtension().getEndpointType())) {
			String subscription;
			// default case
			if (properties.getExtension().isAddDestinationAsSubscriptionToQueue()) {
				subscription = destinationName;
				if (properties.getExtension().getQueueAdditionalSubscriptions().length > 0) {
					throw new IllegalArgumentException("No additional queue subscriptions permitted for topic endpoints");
				}
			} else {
				if (properties.getExtension().getQueueAdditionalSubscriptions().length != 1)
					throw new IllegalArgumentException("Exactly one subscription must be provided for topic endpoints");
					subscription = properties.getExtension().getQueueAdditionalSubscriptions()[0];
				}
				consumerFlowProperties.setNewSubscription(JCSMPFactory.onlyInstance().createTopic(subscription));
			}

			return consumerFlowProperties;
    }

	public static boolean isAnonEndpoint(String groupName) {
		return !StringUtils.hasText(groupName);
	}

	public static boolean isDurableEndpoint(String groupName) {
		return !isAnonEndpoint(groupName);
	}

	public static String getTopicName(String baseTopicName, SolaceCommonProperties properties) {
		return baseTopicName;
	}

	public static String getQueueName(String topicName, String groupName, ExtendedProducerProperties<SolaceProducerProperties> properties) {
		String queueNameExpression = properties.getExtension().getQueueNameExpressionsForRequiredGroups().getOrDefault(groupName, properties.getExtension().getQueueNameExpression());
		return resolveQueueNameExpression(queueNameExpression, new ExpressionContextRoot(groupName, topicName, properties));
	}

	public static QueueNames getQueueNames(String topicName, String groupName,
										   ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties, boolean isAnonymous) {
		final String physicalGroupName = isAnonymous ? UUID.randomUUID().toString() : groupName;
		ExpressionContextRoot root = new ExpressionContextRoot(physicalGroupName, topicName, isAnonymous, consumerProperties);

		String resolvedQueueName = resolveQueueNameExpression(consumerProperties.getExtension().getQueueNameExpression(), root);
		String resolvedErrorQueueName = resolveQueueNameExpression(consumerProperties.getExtension().getErrorQueueNameExpression(), root);

		return new QueueNames(resolvedQueueName, resolvedErrorQueueName, physicalGroupName);
	}

	private static String resolveQueueNameExpression(String expression, ExpressionContextRoot root) {
		try {
			EvaluationContext evaluationContext = new StandardEvaluationContext(root);
			ExpressionParser parser = new SpelExpressionParser();
			Expression queueNameExp = parser.parseExpression(expression);
			String resolvedQueueName = (String) queueNameExp.getValue(evaluationContext);
			validateQueueName(resolvedQueueName, expression);
			return resolvedQueueName != null ? resolvedQueueName.trim() : null;
		} catch (ExpressionException e) {
			throw new ProvisioningException(String.format("Failed to evaluate Spring expression: %s", expression), e);
		}
	}

	private static void validateQueueName(String name, String expression) {
		if (!StringUtils.hasText(name)) {
			throw new ProvisioningException(String.format("Invalid SpEL expression %s as it resolves to a String that does not contain actual text.",
					expression));
		}
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
}
