package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.util.RetryableTaskService.RetryableTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;

public class RetryableTaskServiceTest {
	private RetryableTaskService taskService;
	private static final Log logger = LogFactory.getLog(RetryableTaskServiceTest.class);

	@Before
	public void setUp() {
		taskService = new RetryableTaskService();
	}

	@After
	public void tearDown() {
		taskService.close();
	}

	@Test
	public void testSubmit() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		taskService.submit(() -> {
			latch.countDown();
			return true;
		});

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
	}

	@Test
	public void testRetry() throws Exception {
		CountDownLatch latch = new CountDownLatch(3);
		taskService.submit(() -> {
			logger.info("Attempts remaining: " + latch.getCount());
			latch.countDown();
			return latch.getCount() <= 0;
		});

		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
	}

	@Test
	public void testRetryOrder() throws Exception {
		final int numTasks = 3;
		final int numAttempts = 3;
		final int taskToEarlyTerminate = 1;

		CountDownLatch startLatch = new CountDownLatch(1);
		List<CountDownLatch> retryLatches = new ArrayList<>();

		for (int i = 0; i < numTasks; i++) {
			retryLatches.add(new CountDownLatch(taskToEarlyTerminate == i ? numAttempts - 1 : numAttempts));
		}

		ConcurrentLinkedQueue<Integer> receiveOrder = new ConcurrentLinkedQueue<>();
		List<Integer> expectedReceiveOrder = new ArrayList<>();

		for (int attempt = 1; attempt <= numAttempts; attempt++) {
			for (int taskId = 0; taskId < numTasks; taskId++) {
				if (taskToEarlyTerminate == taskId && attempt >= numAttempts) continue;
				expectedReceiveOrder.add(taskId);
			}
		}

		BiFunction<Integer, CountDownLatch, RetryableTask> taskGenerator = (id, retryLatch) -> () -> {
			startLatch.await();
			receiveOrder.add(id);
			logger.info(String.format("Task %s: Attempt %s", id, retryLatch.getCount()));
			retryLatch.countDown();
			if (retryLatch.getCount() <= 0) {
				logger.info(String.format("Task %s: Done", id));
				return true;
			} else {
				return false;
			}
		};

		for (int i = 0; i < numTasks; i++) {
			taskService.submit(taskGenerator.apply(i, retryLatches.get(i)));
		}

		assertThat(receiveOrder).isEmpty();
		startLatch.countDown();

		for (CountDownLatch retryLatch : retryLatches) {
			assertThat(retryLatch.await(1, TimeUnit.MINUTES)).isTrue();
		}
		assertThat(receiveOrder.toArray(new Integer[0])).isEqualTo(expectedReceiveOrder.toArray());
	}
}
