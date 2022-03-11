package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil.QueueNames;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SolaceProvisioningUtilTest {
	@Test
	public void testConsumerErrorQueueNameOverride() {
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
		consumerProperties.getExtension().setErrorQueueNameOverride("some-custom-named-error-queue-name");
		QueueNames queueNames = SolaceProvisioningUtil.getQueueNames("topic", "group",
				consumerProperties, false);
		assertEquals(consumerProperties.getExtension().getErrorQueueNameOverride(), queueNames.getErrorQueueName());
	}

	@Test
	public void testAnonConsumerErrorQueueNameOverride() {
		ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
		consumerProperties.getExtension().setErrorQueueNameOverride("some-custom-named-error-queue-name");
		QueueNames queueNames = SolaceProvisioningUtil.getQueueNames("topic", null,
				consumerProperties, true);
		assertEquals(consumerProperties.getExtension().getErrorQueueNameOverride(), queueNames.getErrorQueueName());
	}
}
