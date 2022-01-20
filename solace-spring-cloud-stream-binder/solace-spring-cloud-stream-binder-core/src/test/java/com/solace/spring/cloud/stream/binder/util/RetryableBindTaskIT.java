package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
public class RetryableBindTaskIT {
	private FlowReceiverContainer flowReceiverContainer;

	private static final Log logger = LogFactory.getLog(RetryableBindTaskIT.class);

	@BeforeEach
	public void setUp(JCSMPSession jcsmpSession, Queue queue) throws Exception {
		flowReceiverContainer = new FlowReceiverContainer(jcsmpSession, queue.getName(), new EndpointProperties());
	}

	@AfterEach
	public void tearDown() {
		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverContainer.FlowReceiverReference::get)
					.ifPresent(Consumer::close);
		}
	}

	@Test
	public void testRun() {
		RetryableBindTask task = new RetryableBindTask(flowReceiverContainer);
		assertFalse(flowReceiverContainer.isBound());
		assertTrue(task.run(1));
		assertTrue(flowReceiverContainer.isBound());
	}

	@Test
	public void testRunFail(JCSMPProperties jcsmpProperties, Queue queue, SempV2Api sempV2Api) throws Exception {
		String vpnName = jcsmpProperties.getStringProperty(JCSMPProperties.VPN_NAME);
		logger.info(String.format("Shutting down egress for queue %s", queue.getName()));
		sempV2Api.config().updateMsgVpnQueue(vpnName, queue.getName(), new ConfigMsgVpnQueue().egressEnabled(false),
				null);
		Thread.sleep(TimeUnit.SECONDS.toMillis(3));

		RetryableBindTask task = new RetryableBindTask(flowReceiverContainer);
		assertFalse(flowReceiverContainer.isBound());
		assertFalse(task.run(1));
		assertFalse(flowReceiverContainer.isBound());
	}

	@Test
	public void testEquals() {
		RetryableBindTask task1 = new RetryableBindTask(flowReceiverContainer);
		RetryableBindTask task2 = new RetryableBindTask(flowReceiverContainer);
		assertEquals(task1, task2);
		assertEquals(task1.hashCode(), task1.hashCode());
		assertThat(new HashSet<>(Collections.singleton(task1)), contains(task2));
	}
}
