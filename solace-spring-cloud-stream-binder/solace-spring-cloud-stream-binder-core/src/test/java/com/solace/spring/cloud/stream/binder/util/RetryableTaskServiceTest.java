package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.util.RetryableTaskService.RetryableTask;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension.ExecSvc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ExecutorServiceExtension.class)
public class RetryableTaskServiceTest {
	private RetryableTaskService taskService;
	private static final Log logger = LogFactory.getLog(RetryableTaskServiceTest.class);

	@BeforeEach
	public void setUp() {
		taskService = new RetryableTaskService();
	}

	@AfterEach
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
	public void testRetryMultiple() throws Exception {
		final int numTasks = 3;
		final int numAttempts = 3;
		final int taskToEarlyTerminate = 1;

		CountDownLatch startLatch = new CountDownLatch(1);
		List<CountDownLatch> retryLatches = new ArrayList<>();

		for (int i = 0; i < numTasks; i++) {
			retryLatches.add(new CountDownLatch(taskToEarlyTerminate == i ? numAttempts - 1 : numAttempts));
		}

		ConcurrentLinkedQueue<Integer> received = new ConcurrentLinkedQueue<>();
		List<Integer> expectedReceived = new ArrayList<>();

		for (int attempt = 1; attempt <= numAttempts; attempt++) {
			for (int taskId = 0; taskId < numTasks; taskId++) {
				if (taskToEarlyTerminate == taskId && attempt >= numAttempts) continue;
				expectedReceived.add(taskId);
			}
		}

		BiFunction<Integer, CountDownLatch, RetryableTask> taskGenerator = (id, retryLatch) -> attempt -> {
			startLatch.await();
			received.add(id);
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

		assertThat(received).isEmpty();
		startLatch.countDown();

		for (CountDownLatch retryLatch : retryLatches) {
			assertThat(retryLatch.await(1, TimeUnit.MINUTES)).isTrue();
		}
		assertThat(received.toArray(new Integer[0])).containsExactlyInAnyOrderElementsOf(expectedReceived);
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

	@Test
	public void testConcurrentBlocking() throws Exception {
		CountDownLatch latch = new CountDownLatch(2);
		CountDownLatch continueLatch = new CountDownLatch(1);
		RetryableTask task1 = attempt -> {
			logger.info("Starting task 1");
			latch.countDown();
			continueLatch.await();
			return true;
		};

		RetryableTask task2 = attempt -> {
			logger.info("Starting task 2");
			latch.countDown();
			continueLatch.await();
			return true;
		};

		taskService.submit(task1);
		taskService.submit(task2);
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		assertThat(taskService.hasTask(task1)).isTrue();
		assertThat(taskService.hasTask(task2)).isTrue();
		continueLatch.countDown();
		Thread.sleep(500);
		assertThat(taskService.hasTask(task1)).isFalse();
		assertThat(taskService.hasTask(task2)).isFalse();
	}

	@Test
	public void testBlockIfPoolSizeExceeded(@ExecSvc ExecutorService executorService) throws Exception {
		CountDownLatch latch = new CountDownLatch(5);
		CountDownLatch continueLatch = new CountDownLatch(1);
		List<RetryableTask> tasks = IntStream.range(0, (int) latch.getCount())
				.mapToObj(i -> (RetryableTask) attempt -> {
			logger.info("Starting task " + i);
			latch.countDown();
			continueLatch.await();
			return true;
		}).collect(Collectors.toList());

		tasks.forEach(taskService::submit);
		assertThat(latch.await(1, TimeUnit.MINUTES)).isTrue();
		assertThat(tasks).extracting(t -> taskService.hasTask(t)).allMatch(t -> t);

		ReentrantLock lock = Mockito.spy(new ReentrantLock());
		AtomicReference<Condition> condition = new AtomicReference<>();
		Mockito.doAnswer(invocation -> {
			condition.set((Condition) invocation.callRealMethod());
			return condition.get();
		}).when(lock).newCondition();

		Future<Void> blockFuture = executorService.submit(() -> {
			taskService.blockIfPoolSizeExceeded(tasks.size() - 1, lock);
			return null;
		});

		retryAssert(() -> {
			Mockito.verify(lock).newCondition();
			assertThat(condition).doesNotHaveValue(null);
			lock.lock();
			try {
				assertThat(lock.getWaitQueueLength(condition.get())).isEqualTo(1);
			} finally {
				lock.unlock();
			}
		});

		continueLatch.countDown();

		retryAssert(() -> assertThat(tasks).extracting(t -> taskService.hasTask(t)).allMatch(t -> !t));
		logger.info("Waiting for cached worker threads to cleanup");
		assertThat(blockFuture)
				.as("Cached threads should have cleaned up after 1 min and unblocked this")
				.succeedsWithin(5, TimeUnit.MINUTES);
		lock.lock();
		try {
			assertThat(lock.getWaitQueueLength(condition.get())).isEqualTo(0);
		} finally {
			lock.unlock();
		}
	}
}
