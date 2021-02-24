package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil.QueueNames;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SolaceProvisioningUtilTest {
	@Test
	public void testConsumerErrorQueueNameOverride() {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setErrorQueueNameOverride("some-custom-named-error-queue-name");
		QueueNames queueNames = SolaceProvisioningUtil.getQueueNames("topic", "group",
				consumerProperties, false);
		assertEquals(consumerProperties.getErrorQueueNameOverride(), queueNames.getErrorQueueName());
	}

	@Test
	public void testAnonConsumerErrorQueueNameOverride() {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setErrorQueueNameOverride("some-custom-named-error-queue-name");
		QueueNames queueNames = SolaceProvisioningUtil.getQueueNames("topic", null,
				consumerProperties, true);
		assertEquals(consumerProperties.getErrorQueueNameOverride(), queueNames.getErrorQueueName());
	}
}
