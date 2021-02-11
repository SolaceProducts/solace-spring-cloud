package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service for delegating retryable tasks.
 */
public class RetryableTaskService {
	private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
	private static final Log logger = LogFactory.getLog(RetryableTaskService.class);

	public void submit(RetryableTask task) {
		submit(task, 5L, TimeUnit.SECONDS);
	}

	public void submit(RetryableTask task, long retryInterval, TimeUnit unit) {
		executorService.submit(new RetryableTaskWrapper(executorService, task, retryInterval, unit));
	}

	public void close() {
		executorService.shutdownNow();
	}

	private static class RetryableTaskWrapper implements Runnable {
		private final RetryableTask task;
		private final ScheduledExecutorService executorService;
		private final long retryInterval;
		private final TimeUnit unit;

		public RetryableTaskWrapper(ScheduledExecutorService executorService, RetryableTask task, long retryInterval,
									TimeUnit unit) {
			this.task = task;
			this.executorService = executorService;
			this.retryInterval = retryInterval;
			this.unit = unit;
		}

		@Override
		public void run() {
			try {
				if (!task.run()) {
					executorService.schedule(this, retryInterval, unit);
				}
			} catch (InterruptedException e) {
				logger.warn("Interrupt received. Aborting task.", e);
			}
		}
	}

	@FunctionalInterface
	public interface RetryableTask {

		/**
		 *
		 * @return true if successful, false to retry
		 * @throws InterruptedException abort
		 */
		boolean run() throws InterruptedException;
	}
}
