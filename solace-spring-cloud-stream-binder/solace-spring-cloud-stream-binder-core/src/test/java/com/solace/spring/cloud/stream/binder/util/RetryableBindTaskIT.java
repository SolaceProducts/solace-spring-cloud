package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueue;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class RetryableBindTaskIT extends ITBase {
	private String vpnName;
	private Queue queue;
	private FlowReceiverContainer flowReceiverContainer;

	private static final Log logger = LogFactory.getLog(RetryableBindTaskIT.class);

	@Before
	public void setUp() throws Exception {
		vpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
		queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
		jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		flowReceiverContainer = new FlowReceiverContainer(jcsmpSession, queue.getName(), new EndpointProperties());
	}

	@After
	public void tearDown() throws Exception {
		if (flowReceiverContainer != null) {
			Optional.ofNullable(flowReceiverContainer.getFlowReceiverReference())
					.map(FlowReceiverContainer.FlowReceiverReference::get)
					.ifPresent(Consumer::close);
		}

		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			jcsmpSession.deprovision(queue, JCSMPSession.WAIT_FOR_CONFIRM);
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
	public void testRunFail() throws Exception {
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
