package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.util.RetryableTaskService.RetryableTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

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
		RetryableTask task = attempt -> {
			latch.countDown();
			return true;
		};

		taskService.submit(task);
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		Thread.sleep(500);
		assertThat(taskService.hasTask(task)).isFalse();
	}

	@Test
	public void testSubmitDuplicate() throws Exception {
		CountDownLatch latch = new CountDownLatch(2);
		RetryableTask task = attempt -> {
			latch.countDown();
			return true;
		};

		taskService.submit(task, 1, TimeUnit.MINUTES);
		taskService.submit(task, 1, TimeUnit.MINUTES);
		assertThat(latch.await(3, TimeUnit.SECONDS)).isFalse();
		assertThat(latch.getCount()).isEqualTo(1);
		Thread.sleep(500);
		assertThat(taskService.hasTask(task)).isFalse();
	}

	@Test
	public void testRetry() throws Exception {
		CountDownLatch latch = new CountDownLatch(3);
		RetryableTask task = attempt -> {
			logger.info("Attempts remaining: " + latch.getCount());
			latch.countDown();
			return latch.getCount() <= 0;
		};

		taskService.submit(task);
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		Thread.sleep(500);
		assertThat(taskService.hasTask(task)).isFalse();
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

		BiFunction<Integer, CountDownLatch, RetryableTask> taskGenerator = (id, retryLatch) -> attempt -> {
			startLatch.await();
			receiveOrder.add(id);
			logger.info(String.format("Task %s: Attempt %s", id, attempt));
			retryLatch.countDown();
			if (retryLatch.getCount() <= 0) {
				logger.info(String.format("Task %s: Done", id));
				return true;
			} else {
				return false;
			}
		};

		Set<RetryableTask> tasks = new HashSet<>();
		for (int i = 0; i < numTasks; i++) {
			RetryableTask task = taskGenerator.apply(i, retryLatches.get(i));
			tasks.add(task);
			taskService.submit(task);
		}

		assertThat(receiveOrder).isEmpty();
		startLatch.countDown();

		for (CountDownLatch retryLatch : retryLatches) {
			assertThat(retryLatch.await(1, TimeUnit.MINUTES)).isTrue();
		}
		assertThat(receiveOrder.toArray(new Integer[0])).isEqualTo(expectedReceiveOrder.toArray());
		for (RetryableTask task : tasks) {
			assertThat(taskService.hasTask(task)).isFalse();
		}
	}

	@Test
	public void testTaskInterrupt() throws Exception {
		RetryableTask task = attempt -> {
			throw new InterruptedException("Test");
		};

		taskService.submit(task);
		Thread.sleep(500);
		assertThat(taskService.hasTask(task)).isFalse();
	}

	@Test
	public void testShutdown() throws Exception {
		CountDownLatch latch1 = new CountDownLatch(1);
		RetryableTask task1 = attempt -> {
			latch1.countDown();
			Thread.sleep(TimeUnit.MINUTES.toMillis(1));
			return false;
		};
		RetryableTask task2 = attempt -> false;

		taskService.submit(task1);
		taskService.submit(task2);
		assertThat(latch1.await(1, TimeUnit.MINUTES)).isTrue();
		taskService.close();
		Thread.sleep(500);
		assertThat(taskService.hasTask(task1)).isFalse();
		assertThat(taskService.hasTask(task2)).isFalse();
	}

	@Test
	public void testSubmitAfterClose() {
		taskService.close();
		assertThrows(RejectedExecutionException.class, () -> taskService.submit(attempt -> true));
	}

	@Test
	public void testSubmitNull() {
		taskService.submit(null);
		assertThrows(NullPointerException.class, () -> taskService.hasTask(null));
	}
}
