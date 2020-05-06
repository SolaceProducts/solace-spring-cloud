package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Keep-alive service for manually keeping the JVM alive for JCSMP services.
 */
public class JCSMPKeepAlive {
	final ExecutorService executor = Executors.newFixedThreadPool(1);
	private final Map<Pair<Class<?>, String>, KeepAliveRunnable<?>> keyMap = new HashMap<>();
	private BlockingRunnable blockTask;

	private static final Log logger = LogFactory.getLog(JCSMPKeepAlive.class);

	/**
	 * Creates a new task to keep the JVM alive.
	 * @param type the type of the object of type T we're keeping alive for
	 * @param id the id of the object of type T we're keeping alive for
	 * @param <T> the type of the object we're keeping alive for
	 * @return true if successfully added a keep-alive, false otherwise (i.e. already created/running)
	 */
	public <T> boolean create(Class<T> type, String id) {
		Pair<Class<?>, String> key = new Pair<>(type, id);

		logger.info(String.format("Creating keep-alive task for <id: %s, type: %s>", id, type.getName()));

		synchronized (keyMap) {
			if (keyMap.containsKey(key) && keyMap.get(key).isRunning()) {
				logger.info(String.format("A keep-alive task already exists for <id: %s, type: %s>", id, type.getName()));
				return false;
			}

			if (blockTask == null || blockTask.isDone()) {
				blockTask = new BlockingRunnable();
				logger.info(String.format("Creating blocker task %s", blockTask.getId()));
				executor.execute(blockTask);
			}

			KeepAliveRunnable<T> runnable =  new KeepAliveRunnable<>(type, id);
			keyMap.put(key, runnable);
			executor.execute(runnable);
		}

		return true;
	}

	/**
	 * Stops and removes the keep-alive task. Does nothing if the task doesn't exist.
	 * @param type the type of the object of type T we're keeping alive for
	 * @param id the id of the object of type T we're keeping alive for
	 * @param <T> the type of the object we're keeping alive for
	 */
	public <T> void stop(Class<T> type, String id) {
		Pair<Class<?>, String> key = new Pair<>(type, id);

		logger.info(String.format("Will stop keep-alive task for <id: %s, type: %s>", id, type.getName()));

		synchronized (keyMap) {
			if (!keyMap.containsKey(key)) {
				logger.info(String.format("No keep-alive task was found for <id: %s, type: %s>, nothing to do",
						id, type.getName()));
				return;
			}

			keyMap.get(key).stop();
			keyMap.remove(key);

			if (keyMap.isEmpty()) {
				blockTask.terminate();
			} else {
				blockTask.unblock();
			}
		}
	}

	final class KeepAliveRunnable<T> implements Runnable {
		private final Log logger = LogFactory.getLog(KeepAliveRunnable.class);
		private final AtomicBoolean isAlive = new AtomicBoolean(true);
		private final Class<T> type;
		private final String id;

		private KeepAliveRunnable(Class<T> type, String id) {
			this.type = type;
			this.id = id;
		}

		@Override
		public void run() {
			logger.info(String.format("Checking liveliness of <id: %s, type: %s>", id, type.getName()));
			synchronized (isAlive) {
				if (isRunning()) {
					logger.info(String.format("Re-queuing keep-alive task for <id: %s, type: %s>", id, type.getName()));
					executor.execute(this);
				} else {
					logger.info(String.format("Ended keep-alive task for <id: %s, type: %s>", id, type.getName()));
				}
			}
		}

		public void stop() {
			logger.info(String.format("Stopping keep-alive task for <id: %s, type: %s>", id, type.getName()));
			synchronized (isAlive) {
				isAlive.set(false);
			}
		}

		public boolean isRunning() {
			synchronized (isAlive) {
				return isAlive.get();
			}
		}

		public Class<T> getType() {
			return type;
		}

		public String getId() {
			return id;
		}
	}

	final class BlockingRunnable implements Runnable {
		private final Log logger = LogFactory.getLog(BlockingRunnable.class);
		private final String id = UUID.randomUUID().toString();
		private final AtomicBoolean keepAlive = new AtomicBoolean(true);

		private BlockingRunnable() {
		}

		@Override
		public void run() {
			synchronized (keepAlive) {
				logger.info(String.format("Starting blocker task %s", id));

				if (keepAlive.get()) {
					try {
						keepAlive.wait();
					} catch (InterruptedException e) {
						logger.warn(String.format("Received interrupt, ending blocker task %s", id), e);
						return;
					}
					logger.info(String.format("Unblocking blocker task %s", id));
				} else {
					logger.info(String.format("Blocker task %s already told to die, skipping wait", id));
				}

				if (keepAlive.get()) {
					logger.info(String.format("Re-queuing blocker task %s", id));
					executor.execute(this);
				} else {
					logger.info(String.format("Ending blocker task %s", id));
				}
			}
		}

		public void unblock() {
			synchronized (keepAlive) {
				keepAlive.notifyAll();
			}
		}

		public void terminate() {
			synchronized (keepAlive) {
				logger.info(String.format("Scheduling termination of blocker task %s", id));
				keepAlive.set(false);
				keepAlive.notifyAll();
			}
		}

		public boolean isDone() {
			synchronized (keepAlive) {
				return !keepAlive.get();
			}
		}

		String getId() {
			return id;
		}
	}

	private static final class Pair<T1,T2> {
		private final T1 t1;
		private final T2 t2;

		public Pair(T1 t1, T2 t2) {
			this.t1 = t1;
			this.t2 = t2;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Pair<?, ?> pair = (Pair<?, ?>) o;
			return t1.equals(pair.t1) &&
					t2.equals(pair.t2);
		}

		@Override
		public int hashCode() {
			return Objects.hash(t1, t2);
		}
	}
}
