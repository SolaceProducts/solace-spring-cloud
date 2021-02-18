package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service for delegating retryable tasks.
 */
public class RetryableTaskService {
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final ExecutorService workerService = Executors.newCachedThreadPool();
	private static final Log logger = LogFactory.getLog(RetryableTaskService.class);
	private final Set<RetryableTask> tasks = ConcurrentHashMap.newKeySet();

	public void submit(RetryableTask task) {
		submit(task, 5L, TimeUnit.SECONDS);
	}

	public void submit(RetryableTask task, long retryInterval, TimeUnit unit) {
		if (task == null) {
			return;
		}

		if (hasTask(task)) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Skipping task submission. Task already exists: %s", task));
			}
			return;
		}

		tasks.add(task);
		scheduler.execute(new RetryableTaskWrapper(this, task, retryInterval, unit));
	}

	public void close() {
		scheduler.shutdownNow();
		workerService.shutdownNow();
		if (!tasks.isEmpty()) {
			logger.info(String.format("Task service shutdown. Cancelling tasks: %s", tasks));
		}
		tasks.clear();
	}

	public boolean hasTask(RetryableTask task) {
		return tasks.contains(task);
	}

	private static class RetryableTaskWrapper implements Runnable {
		private final RetryableTask task;
		private final RetryableTaskService taskService;
		private final long retryInterval;
		private final TimeUnit unit;
		private final AtomicInteger attempt = new AtomicInteger(0);

		public RetryableTaskWrapper(RetryableTaskService taskService, RetryableTask task, long retryInterval,
									TimeUnit unit) {
			this.task = task;
			this.taskService = taskService;
			this.retryInterval = retryInterval;
			this.unit = unit;
		}

		@Override
		public void run() {
			taskService.workerService.execute(this::runWorker);
		}

		private void runWorker() {
			if (Thread.currentThread().isInterrupted()) {
				logger.info(String.format("Interrupt received. Aborting task: %s", task));
				taskService.tasks.remove(task);
				return;
			}

			try {
				if (task.run(attempt.incrementAndGet())) {
					taskService.tasks.remove(task);
				} else {
					taskService.scheduler.schedule(this, retryInterval, unit);
				}
			} catch (InterruptedException e) {
				logger.info(String.format("Interrupt received. Aborting task: %s", task), e);
				taskService.tasks.remove(task);
			}
		}
	}

	@FunctionalInterface
	public interface RetryableTask {

		/**
		 * The action to perform on each attempt.
		 * @param attempt the attempt count. Starts at 1.
		 * @return true if successful, false to retry
		 * @throws InterruptedException abort
		 */
		boolean run(int attempt) throws InterruptedException;
	}
}
