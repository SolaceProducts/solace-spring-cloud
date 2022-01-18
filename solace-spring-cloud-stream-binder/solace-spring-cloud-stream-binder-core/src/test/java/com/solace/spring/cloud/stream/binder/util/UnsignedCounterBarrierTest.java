package com.solace.spring.cloud.stream.binder.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnsignedCounterBarrierTest {
	@Test
	public void testIncrement() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier();
		unsignedCounterBarrier.increment();
		assertEquals(1, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testIncrementUnsigned() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(-5);
		assertEquals(-5, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.increment();
		assertEquals(-4, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testIncrementSignedLimit() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(Long.MAX_VALUE);
		assertEquals(Long.MAX_VALUE, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.increment();
		assertEquals(Long.MIN_VALUE, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testIncrementUnsignedLimit() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(-1);
		assertEquals(-1, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.increment();
		assertEquals(-1, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testDecrement() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(2);
		assertEquals(2, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.decrement();
		assertEquals(1, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.decrement();
		assertEquals(0, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testDecrementUnsigned() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(-5);
		assertEquals(-5, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.decrement();
		assertEquals(-6, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testDecrementSignedLimit() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(Long.MIN_VALUE);
		assertEquals(Long.MIN_VALUE, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.decrement();
		assertEquals(Long.MAX_VALUE, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testDecrementUnsignedLimit() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier();
		assertEquals(0, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.decrement();
		assertEquals(0, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testReset() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(2);
		assertEquals(2, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.reset();
		assertEquals(0, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testResetUnsigned() {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(-1);
		assertEquals(-1, unsignedCounterBarrier.getCount());
		unsignedCounterBarrier.reset();
		assertEquals(0, unsignedCounterBarrier.getCount());
	}

	@Test
	public void testResetTriggersConcurrentAwaitEmpty() throws Exception {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier(5);
		int concurrency = 5;

		ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
		try {
			ArrayList<Future<Boolean>> futures = new ArrayList<>(concurrency);
			for (int i = 0; i < concurrency; i++) {
				futures.add(executorService.submit(() -> unsignedCounterBarrier.awaitEmpty(-1, TimeUnit.DAYS)));
			}
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			for (Future<?> future : futures) {
				assertFalse(future.isDone());
			}

			unsignedCounterBarrier.reset();
			for (Future<Boolean> future : futures) {
				assertTrue(future.get(1, TimeUnit.MINUTES));
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testConcurrentAwaitEmpty() throws Exception {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier();
		unsignedCounterBarrier.increment();

		int concurrency = 5;

		ExecutorService executorService = Executors.newFixedThreadPool(concurrency);
		try {
			ArrayList<Future<Boolean>> futures = new ArrayList<>(concurrency);
			for (int i = 0; i < concurrency; i++) {
				futures.add(executorService.submit(() -> unsignedCounterBarrier.awaitEmpty(-1, TimeUnit.DAYS)));
			}
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			for (Future<?> future : futures) {
				assertFalse(future.isDone());
			}

			unsignedCounterBarrier.decrement();
			for (Future<Boolean> future : futures) {
				assertTrue(future.get(1, TimeUnit.MINUTES));
			}
		} finally {
			executorService.shutdownNow();
		}
	}

	@Test
	public void testAwaitEmptyTimeout() throws Exception {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier();
		unsignedCounterBarrier.increment();

		long timeout = 3;
		TimeUnit unit = TimeUnit.SECONDS;
		long expectedExpiry = unit.toMillis(timeout) + System.currentTimeMillis();
		assertFalse(unsignedCounterBarrier.awaitEmpty(timeout, unit));
		assertThat(System.currentTimeMillis()).isGreaterThanOrEqualTo(expectedExpiry);
	}

	@Test
	public void testAwaitEmptyNoTimeout() throws Exception {
		UnsignedCounterBarrier unsignedCounterBarrier = new UnsignedCounterBarrier();
		unsignedCounterBarrier.increment();
		long expectedMaxExpiry = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1); // being lenient
		assertFalse(unsignedCounterBarrier.awaitEmpty(0, TimeUnit.DAYS));
		long currentTime = System.currentTimeMillis();
		assertThat(currentTime).isLessThan(expectedMaxExpiry);
	}
}
