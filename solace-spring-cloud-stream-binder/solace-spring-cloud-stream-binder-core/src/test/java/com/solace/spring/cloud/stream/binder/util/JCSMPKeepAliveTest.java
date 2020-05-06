package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class JCSMPKeepAliveTest {
	private JCSMPKeepAlive keepAlive;
	private ThreadPoolExecutor executor;

	private static final Log logger = LogFactory.getLog(JCSMPKeepAliveTest.class);

	@Before
	public void setup() {
		keepAlive = new JCSMPKeepAlive();
		executor = (ThreadPoolExecutor) keepAlive.executor;
		printExecutorStats();
	}

	@Test
	public void testBasic() throws Exception {
		assertEquals(Integer.MAX_VALUE, executor.getQueue().remainingCapacity());

		printExecutorStats();

		Dummy obj0 = new Dummy(UUID.randomUUID().toString());
		keepAlive.create(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj0.getId(), obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj0.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj0.id, obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(2, executor.getCompletedTaskCount());
	}

	@Test
	public void testMultiFast() throws Exception {
		List<Dummy> objs = new ArrayList<>();

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 3; j++) {
				Dummy obj = new Dummy(UUID.randomUUID().toString());
				objs.add(obj);
				keepAlive.create(obj.getClass(), obj.getId());
				logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj.getId(), obj.getClass().getName()));
			}

			for (Dummy obj : objs) {
				keepAlive.stop(obj.getClass(), obj.getId());
				logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));
			}

			printExecutorStats();
		}

		Thread.sleep(1000);
		printExecutorStats();
		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertThat(executor.getCompletedTaskCount(), greaterThanOrEqualTo((long) objs.size()));
		assertThat(executor.getCompletedTaskCount(), lessThanOrEqualTo((long) sigma(objs.size() + 1)));
	}

	@Test
	public void testConcurrent() throws Exception {
		for (int repeatTest = 1; repeatTest < 10; repeatTest++) {
			int maxNumIters = 10;
			List<Dummy> objs = new ArrayList<>();
			List<Runnable> runnables = new ArrayList<>();
			CountDownLatch latch1 = new CountDownLatch(maxNumIters);
			CountDownLatch latch2 = new CountDownLatch(maxNumIters);
			CountDownLatch latch3 = new CountDownLatch(maxNumIters);
			CountDownLatch latch4 = new CountDownLatch(maxNumIters);
			CountDownLatch latch5 = new CountDownLatch(maxNumIters);

			for (int i = 0; i < maxNumIters; i++) {
				Dummy obj = new Dummy(UUID.randomUUID().toString());
				objs.add(obj);

				runnables.add(() -> {
					try {
						latch1.countDown();
						assertTrue(latch1.await(5, TimeUnit.SECONDS));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}

					keepAlive.create(obj.getClass(), obj.getId());
					logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj.getId(), obj.getClass().getName()));

					latch2.countDown();

					keepAlive.stop(obj.getClass(), obj.getId());
					logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));

					printExecutorStats();
				});

				runnables.add(() -> {
					try {
						assertTrue(latch2.await(5, TimeUnit.SECONDS));
						latch3.countDown();
						assertTrue(latch3.await(5, TimeUnit.SECONDS));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}

					keepAlive.create(obj.getClass(), obj.getId());
					logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj.getId(), obj.getClass().getName()));

					keepAlive.stop(obj.getClass(), obj.getId());
					logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));
					latch4.countDown();

					printExecutorStats();
				});

				runnables.add(() -> {
					try {
						assertTrue(latch4.await(5, TimeUnit.SECONDS));
						latch5.countDown();
						assertTrue(latch5.await(5, TimeUnit.SECONDS));
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					}

					keepAlive.create(obj.getClass(), obj.getId());
					logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj.getId(), obj.getClass().getName()));

					keepAlive.stop(obj.getClass(), obj.getId());
					logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));

					printExecutorStats();
				});
			}

			ExecutorService testExecutor = null;
			try {
				testExecutor = Executors.newFixedThreadPool(runnables.size());
				runnables.forEach(testExecutor::submit);
				testExecutor.shutdown();
				assertTrue(testExecutor.awaitTermination(5, TimeUnit.SECONDS));
			} finally {
				if (testExecutor != null && !testExecutor.isShutdown()) {
					testExecutor.shutdownNow();
				}
			}

			Thread.sleep(1000);
			printExecutorStats();
			assertEquals(0, executor.getActiveCount());
			assertEquals(0, executor.getQueue().size());
			assertThat(executor.getCompletedTaskCount(), greaterThanOrEqualTo((long) repeatTest * objs.size()));
			assertThat(executor.getCompletedTaskCount(), lessThanOrEqualTo((long) repeatTest * sigma(objs.size() + 1)));
		}
	}

	@Test
	public void testMultiStopHeadFirst() throws Exception {
		List<Dummy> objs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Dummy obj = new Dummy(UUID.randomUUID().toString());
			objs.add(obj);
			keepAlive.create(obj.getClass(), obj.getId());
		}

		Thread.sleep(1000);
		logger.info("All keep-alives have been created");
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(objs.size(), executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(objs.get(0).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		for (int i = 0; i < objs.size(); i++) {
			Dummy obj = objs.get(i);

			keepAlive.stop(obj.getClass(), obj.getId());
			Thread.sleep(1000);
			logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));
			printExecutorStats();

			assertEquals(objs.size() - i - 1, executor.getQueue().size());
			assertEquals(sigma(objs.size() + 1) - sigma(objs.size() - i), executor.getCompletedTaskCount());

			Runnable queueHead = executor.getQueue().peek();
			if (queueHead != null) {
				assertEquals(1, executor.getActiveCount());
				assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, queueHead.getClass());
				assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getType());
				assertEquals(objs.get(i + 1).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getId());
				assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));
			} else {
				assertEquals(0, executor.getActiveCount());
			}
		}
	}

	@Test
	public void testMultiStopTailFirst() throws Exception {
		List<Dummy> objs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Dummy obj = new Dummy(UUID.randomUUID().toString());
			objs.add(obj);
			keepAlive.create(obj.getClass(), obj.getId());
		}

		Thread.sleep(1000);
		logger.info("All keep-alives have been created");
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(objs.size(), executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(objs.get(0).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		List<Dummy> reverseObjs = new ArrayList<>(objs);
		Collections.reverse(reverseObjs);

		for (int i = 0; i < reverseObjs.size(); i++) {
			Dummy obj = reverseObjs.get(i);

			keepAlive.stop(obj.getClass(), obj.getId());
			Thread.sleep(1000);
			logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));
			printExecutorStats();

			assertEquals(objs.size() - i - 1, executor.getQueue().size());
			assertEquals(sigma(objs.size() + 1) - sigma(objs.size() - i), executor.getCompletedTaskCount());

			Runnable queueHead = executor.getQueue().peek();
			if (queueHead != null) {
				assertEquals(1, executor.getActiveCount());
				assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, queueHead.getClass());
				assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getType());
				assertEquals(objs.get(0).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getId());
				assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));
			} else {
				assertEquals(0, executor.getActiveCount());
			}
		}
	}

	@Test
	public void testMultiStopMiddleFirst() throws Exception {
		List<Dummy> objs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Dummy obj = new Dummy(UUID.randomUUID().toString());
			objs.add(obj);
			keepAlive.create(obj.getClass(), obj.getId());
		}

		Thread.sleep(1000);
		logger.info("All keep-alives have been created");
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(objs.size(), executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(objs.get(0).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		List<Dummy> middleToFrontObjs = new ArrayList<>(objs);
		Collections.reverse(middleToFrontObjs);
		middleToFrontObjs.add(middleToFrontObjs.remove(1));
		Collections.reverse(middleToFrontObjs);
		assertEquals("Test error: expected middle element to come to front", objs.get(1).getId(),
				middleToFrontObjs.get(0).getId());

		for (int i = 0; i < middleToFrontObjs.size(); i++) {
			Dummy obj = middleToFrontObjs.get(i);

			keepAlive.stop(obj.getClass(), obj.getId());
			Thread.sleep(1000);
			logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj.id, obj.getClass().getName()));
			printExecutorStats();

			assertEquals(objs.size() - i - 1, executor.getQueue().size());
			assertEquals(sigma(objs.size() + 1) - sigma(objs.size() - i), executor.getCompletedTaskCount());

			Runnable queueHead = executor.getQueue().peek();
			if (queueHead != null) {
				assertEquals(1, executor.getActiveCount());
				assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, queueHead.getClass());
				assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getType());
				assertEquals(middleToFrontObjs.get(i + 1).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) queueHead).getId());
				assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));
			} else {
				assertEquals(0, executor.getActiveCount());
			}
		}
	}

	@Test
	public void testRefillIdentical() throws Exception {
		Dummy obj0 = new Dummy(UUID.randomUUID().toString());
		keepAlive.create(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj0.getId(), obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj0.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj0.id, obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(2, executor.getCompletedTaskCount());

		keepAlive.create(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj0.getId(), obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(2, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj0.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj0.id, obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(4, executor.getCompletedTaskCount());
	}

	@Test
	public void testRefillUnique() throws Exception {
		Dummy obj0 = new Dummy(UUID.randomUUID().toString());
		keepAlive.create(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj0.getId(), obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj0.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj0.id, obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(2, executor.getCompletedTaskCount());

		Dummy obj1 = new Dummy(UUID.randomUUID().toString());
		keepAlive.create(obj1.getClass(), obj1.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj1.getId(), obj1.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(2, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj1.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj1.getClass(), obj1.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj1.id, obj1.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(4, executor.getCompletedTaskCount());
	}

	@Test
	public void testDifferentTypes() throws Exception {
		Dummy obj0 = new Dummy(UUID.randomUUID().toString());
		Dummy2 obj1 = new Dummy2(obj0.getId());

		assertEquals("Test error: Expected objs to have same IDs", obj0.getId(), obj1.getId());

		keepAlive.create(obj0.getClass(), obj0.getId());
		keepAlive.create(obj1.getClass(), obj1.getId());
		Thread.sleep(1000);
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj0.getId(), obj0.getClass().getName()));
		logger.info(String.format("Created keep-alive for <id: %s, type: %s>", obj1.getId(), obj1.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(2, executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj0.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj0.getClass(), obj0.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj0.id, obj0.getClass().getName()));
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(1, executor.getQueue().size());
		assertEquals(3, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy2.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(obj1.id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		keepAlive.stop(obj1.getClass(), obj1.getId());
		Thread.sleep(1000);
		logger.info(String.format("Stopped keep-alive for <id: %s type: %s>", obj1.id, obj1.getClass().getName()));
		printExecutorStats();

		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(5, executor.getCompletedTaskCount());
	}

	@Test
	public void testInterrupt() throws Exception {
		List<Dummy> objs = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			Dummy obj = new Dummy(UUID.randomUUID().toString());
			objs.add(obj);
			keepAlive.create(obj.getClass(), obj.getId());
		}

		Thread.sleep(1000);
		logger.info("All keep-alives have been created");
		printExecutorStats();

		assertEquals(1, executor.getActiveCount());
		assertEquals(objs.size(), executor.getQueue().size());
		assertEquals(0, executor.getCompletedTaskCount());
		assertEquals(JCSMPKeepAlive.KeepAliveRunnable.class, Objects.requireNonNull(executor.getQueue().peek()).getClass());
		assertEquals(Dummy.class, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getType());
		assertEquals(objs.get(0).id, ((JCSMPKeepAlive.KeepAliveRunnable<?>) Objects.requireNonNull(executor.getQueue().peek())).getId());
		assertThat(executor.getQueue(), not(hasItem(isA(JCSMPKeepAlive.BlockingRunnable.class))));

		logger.info(String.format("Shutting down executor to force an %s", InterruptedException.class.getSimpleName()));
		executor.shutdownNow();
		assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

		printExecutorStats();
		assertTrue(executor.isShutdown());
		assertEquals(0, executor.getActiveCount());
		assertEquals(0, executor.getQueue().size());
		assertEquals(1, executor.getCompletedTaskCount());
	}

	private void printExecutorStats() {
		StringBuilder print = new StringBuilder("Executor Stats:")
				.append("\nActive Count: ")
				.append(executor.getActiveCount())
				.append("\nCompleted Task Count: ")
				.append(executor.getCompletedTaskCount())
				.append("\nQueue Stats: ")
				.append("\n\tSize: ")
				.append(executor.getQueue().size());

		if (executor.getQueue().size() > 0) {
			print.append("\n\tQueue Contents:");
			Runnable[] contents = executor.getQueue().toArray(new Runnable[0]);
			for (int i = 0; i < contents.length; i++) {
				Runnable runnable = contents[i];
				print.append("\n\t\t")
						.append(i)
						.append(':')
						.append("\n\t\t\tClass: ")
						.append(runnable.getClass());
				if (runnable.getClass().equals(JCSMPKeepAlive.KeepAliveRunnable.class)) {
					print.append("\n\t\t\tID: ")
							.append(((JCSMPKeepAlive.KeepAliveRunnable<?>) runnable).getId())
							.append("\n\t\t\tType: ")
							.append(((JCSMPKeepAlive.KeepAliveRunnable<?>) runnable).getType());
				} else if (runnable.getClass().equals(JCSMPKeepAlive.BlockingRunnable.class)) {
					print.append("\n\t\t\tID: ")
							.append(((JCSMPKeepAlive.BlockingRunnable) runnable).getId());
				}
			}
		}

		logger.info(print);
	}

	/**
	 * Calculates the summation from 1 to n.
	 * @param n the upper limit of the summation.
	 * @return The sum of all numbers between 1 and n (inclusive).
	 */
	private int sigma(int n) {
		return n * (n + 1) / 2;
	}

	private static class Dummy {
		private final String id;

		public Dummy(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}
	}

	private static class Dummy2 {
		private final String id;

		public Dummy2(String id) {
			this.id = id;
		}

		public String getId() {
			return id;
		}
	}
}
