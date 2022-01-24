package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solacesystems.jcsmp.EndpointProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.expression.*;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.StringUtils;

import java.util.*;

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

	public static boolean isAnonQueue(String groupName) {
		return !StringUtils.hasText(groupName);
	}

	public static boolean isDurableQueue(String groupName) {
		return !isAnonQueue(groupName);
	}

	public static String getTopicName(String baseTopicName, SolaceCommonProperties properties) {
		return baseTopicName;
	}

	public static String getQueueName(String topicName, String groupName, ExtendedProducerProperties<SolaceProducerProperties> properties) {
		return resolveQueueNameExpression(properties.getExtension().getQueueNameExpression(), new ExpressionContextRoot(groupName, topicName, properties));
	}

	public static QueueNames getQueueNames(String topicName, String groupName,
										   ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties, boolean isAnonymous) {
		final String physicalGroupName = isAnonymous ? UUID.randomUUID().toString() : groupName;
		ExpressionContextRoot root = new ExpressionContextRoot(physicalGroupName, topicName, isAnonymous, consumerProperties);

		String resolvedQueueName = resolveQueueNameExpression(consumerProperties.getExtension().getQueueNameExpression(), root);
		String resolvedErrorQueueName = resolveQueueNameExpression(consumerProperties.getExtension().getErrorQueueNameExpression(), root);

		QueueNames queueNames = new QueueNames(resolvedQueueName, resolvedErrorQueueName, physicalGroupName);

		if (StringUtils.hasText(consumerProperties.getExtension().getErrorQueueNameOverride())) {
			return new QueueNames(queueNames.getConsumerGroupQueueName(),
					consumerProperties.getExtension().getErrorQueueNameOverride(), queueNames.getPhysicalGroupName());
		} else {
			return queueNames;
		}
	}

	private static String resolveQueueNameExpression(String expression, ExpressionContextRoot root) {
		EvaluationContext evaluationContext = new StandardEvaluationContext(root);
		ExpressionParser parser = new SpelExpressionParser();
		Expression queueNameExp = parser.parseExpression(expression);
		return (String) queueNameExp.getValue(evaluationContext);
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
