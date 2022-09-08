package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.Consumer;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
		initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
public class RetryableRebindTaskIT {
	private RetryableTaskService taskService;
	private FlowReceiverContainer flowReceiverContainer;


	@BeforeEach
	public void setUp(JCSMPSession jcsmpSession, Queue queue) throws Exception {
		taskService = Mockito.spy(new RetryableTaskService());
		flowReceiverContainer = Mockito.spy(new FlowReceiverContainer(
				jcsmpSession,
				queue.getName(),
				new EndpointProperties(),
				new FixedBackOff(1, Long.MAX_VALUE),
				lock -> taskService.blockIfPoolSizeExceeded(Long.MAX_VALUE, lock)));
		flowReceiverContainer.bind();
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
