package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.ITBase;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.test.context.ContextConfiguration;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigFileApplicationContextInitializer.class)
public class RetryableRebindTaskIT extends ITBase {
	private RetryableTaskService taskService;
	private Queue queue;
	private FlowReceiverContainer flowReceiverContainer;


	@Before
	public void setUp() throws Exception {
		taskService = Mockito.spy(new RetryableTaskService());
		queue = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(20));
		jcsmpSession.provision(queue, new EndpointProperties(), JCSMPSession.WAIT_FOR_CONFIRM);
		flowReceiverContainer = Mockito.spy(new FlowReceiverContainer(jcsmpSession, queue.getName(),
				new EndpointProperties()));
		flowReceiverContainer.bind();
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
	public void testRun() throws Exception {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);

		assertTrue(task.run(1));
		Mockito.verify(flowReceiverContainer).rebind(flowId, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
		assertTrue(flowReceiverContainer.isBound());
		assertNotEquals(flowId, Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId());
	}

	@Test
	public void testReturnNull() throws Exception {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);

		Mockito.doReturn(null).when(flowReceiverContainer).rebind(flowId, true);

		assertFalse(task.run(1));
		Mockito.verify(flowReceiverContainer).rebind(flowId, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
	}

	@Test
	public void testFail() throws Exception {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);

		Mockito.doThrow(new JCSMPException("test")).when(flowReceiverContainer).rebind(flowId, true);

		assertFalse(task.run(1));
		Mockito.verify(flowReceiverContainer).rebind(flowId, true);
		Mockito.verify(taskService, Mockito.never()).submit(Mockito.any());
	}

	@Test
	public void testFailAndUnbound() throws Exception {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);

		Mockito.doThrow(new JCSMPException("test")).when(flowReceiverContainer).rebind(flowId, true);
		Mockito.doReturn(false).when(flowReceiverContainer).isBound();

		assertTrue(task.run(1));
		Mockito.verify(flowReceiverContainer).rebind(flowId, true);
		Mockito.verify(taskService).submit(new RetryableBindTask(flowReceiverContainer));
	}

	@Test
	public void testFailWhenUnbound() throws Exception {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);

		flowReceiverContainer.unbind();

		assertTrue(task.run(1));
		Mockito.verify(taskService).submit(new RetryableBindTask(flowReceiverContainer));

		retryAssert(() -> {
			assertFalse(taskService.hasTask(new RetryableBindTask(flowReceiverContainer)));
			assertTrue(flowReceiverContainer.isBound());
		}, 1, TimeUnit.MINUTES);
	}

	@Test
	public void testEquals() {
		UUID flowId = Objects.requireNonNull(flowReceiverContainer.getFlowReceiverReference()).getId();
		RetryableRebindTask task1 = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);
		RetryableRebindTask task2 = new RetryableRebindTask(flowReceiverContainer, flowId, taskService);
		assertEquals(task1, task2);
		assertEquals(task1.hashCode(), task1.hashCode());
		assertThat(new HashSet<>(Collections.singleton(task1)), contains(task2));
	}
}
