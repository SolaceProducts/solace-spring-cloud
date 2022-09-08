package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;

/**
 * Service for delegating retryable tasks.
 */
public class RetryableTaskService {
	private final TaskManager taskManager = new TaskManager();
	private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
	private final ThreadPoolExecutor workerService = (ThreadPoolExecutor) Executors.newCachedThreadPool();
	private static final Log logger = LogFactory.getLog(RetryableTaskService.class);

	public void submit(RetryableTask task) {
		submit(task, 5L, TimeUnit.SECONDS);
	}

	public void submit(RetryableTask task, long retryInterval, TimeUnit unit) {
		if (task == null) {
			return;
		}

		if (!taskManager.addTask(task)) {
			if (logger.isTraceEnabled()) {
				logger.trace(String.format("Skipping task submission. Task already exists: %s", task));
			}
			return;
		}

		scheduler.execute(new RetryableTaskWrapper(task, taskManager, scheduler, workerService, retryInterval, unit));
	}

	public void close() {
		scheduler.shutdownNow();
		workerService.shutdownNow();
		taskManager.clearTasks();
	}

	public boolean hasTask(RetryableTask task) {
		return taskManager.containsTask(task);
	}

	public void blockIfPoolSizeExceeded(long poolSizeThreshold, Lock lock) throws InterruptedException {
		taskManager.blockIfPoolSizeExceeded(lock, poolSizeThreshold, workerService::getPoolSize);
	}

	private static class RetryableTaskWrapper implements Runnable {
		private final RetryableTask task;
		private final TaskManager taskManager;
		private final ScheduledExecutorService scheduler;
		private final ExecutorService workerService;
		private final long retryInterval;
		private final TimeUnit unit;
		private final AtomicInteger attempt = new AtomicInteger(0);

		public RetryableTaskWrapper(RetryableTask task,
									TaskManager taskManager,
									ScheduledExecutorService scheduler,
									ExecutorService workerService,
									long retryInterval,
									TimeUnit unit) {
			this.task = task;
			this.taskManager = taskManager;
			this.scheduler = scheduler;
			this.workerService = workerService;
			this.retryInterval = retryInterval;
			this.unit = unit;
		}

		@Override
		public void run() {
			workerService.execute(this::runWorker);
		}

		private void runWorker() {
			if (Thread.currentThread().isInterrupted()) {
				logger.info(String.format("Interrupt received. Aborting task: %s", task));
				taskManager.removeTask(task);
				return;
			}

			try {
				if (task.run(attempt.incrementAndGet())) {
					taskManager.removeTask(task);
				} else {
					scheduler.schedule(this, retryInterval, unit);
				}
			} catch (InterruptedException e) {
				logger.info(String.format("Interrupt received. Aborting task: %s", task), e);
				taskManager.removeTask(task);
			}
		}
	}

	private static class TaskManager {
		private final Set<RetryableTask> tasks = ConcurrentHashMap.newKeySet();
		private final Map<Long, Map<Lock, Condition>> blockConditionsByThreshold = new ConcurrentHashMap<>();

		public boolean addTask(RetryableTask task) {
			return tasks.add(task);
		}

		public void removeTask(RetryableTask task) {
			tasks.remove(task);
			blockConditionsByThreshold.entrySet()
					.stream()
					.filter(thresholdConditions -> tasks.size() < thresholdConditions.getKey())
					.flatMap(thresholdConditions -> thresholdConditions.getValue().entrySet().stream())
					.forEach(blockCondition -> {
						blockCondition.getKey().lock();
						try {
							blockCondition.getValue().signalAll();
						} finally {
							blockCondition.getKey().unlock();
						}
					});
		}

		public boolean containsTask(RetryableTask task) {
			return tasks.contains(task);
		}

		public void clearTasks() {
			if (!tasks.isEmpty()) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Task service shutdown. Cancelling tasks: %s", tasks));
				}
			}
			tasks.clear();
		}

		public void blockIfPoolSizeExceeded(Lock lock, long poolSizeThreshold, Supplier<Integer> poolSizeSupplier)
				throws InterruptedException {
			int poolSize;
			while ((poolSize = poolSizeSupplier.get()) > poolSizeThreshold) {
				if (logger.isDebugEnabled()) {
					logger.debug(String.format("Waiting for worker pool size to decrease: %s > %s threads",
							poolSize, poolSizeThreshold));
				}

				Map<Lock, Condition> thresholdBlockConditions = blockConditionsByThreshold.computeIfAbsent(
						poolSizeThreshold, k -> new ConcurrentHashMap<>());
				Condition blockCondition = thresholdBlockConditions.computeIfAbsent(lock, Lock::newCondition);
				lock.lock();
				try {
					blockCondition.await(1, TimeUnit.MINUTES);
				} finally {
					lock.unlock();
				}
			}
			if (logger.isTraceEnabled()) {
				logger.trace(String.format("Not blocking on pool size: %s <= %s threads", poolSize, poolSizeThreshold));
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
