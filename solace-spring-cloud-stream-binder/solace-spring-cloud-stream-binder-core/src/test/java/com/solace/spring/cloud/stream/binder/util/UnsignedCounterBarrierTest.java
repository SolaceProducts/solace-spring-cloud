package com.solace.spring.cloud.stream.binder.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

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

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			ArrayList<Future<?>> futures = new ArrayList<>(concurrency);
			for (int i = 0; i < concurrency; i++) {
				futures.add(executorService.submit(() -> {
					unsignedCounterBarrier.awaitEmpty();
					return null;
				}));
			}
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			for (Future<?> future : futures) {
				assertFalse(future.isDone());
			}

			unsignedCounterBarrier.reset();
			for (Future<?> future : futures) {
				future.get(1, TimeUnit.MINUTES);
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

		ExecutorService executorService = Executors.newSingleThreadExecutor();
		try {
			ArrayList<Future<?>> futures = new ArrayList<>(concurrency);
			for (int i = 0; i < concurrency; i++) {
				futures.add(executorService.submit(() -> {
					unsignedCounterBarrier.awaitEmpty();
					return null;
				}));
			}
			executorService.shutdown();

			Thread.sleep(TimeUnit.SECONDS.toMillis(5));
			for (Future<?> future : futures) {
				assertFalse(future.isDone());
			}

			unsignedCounterBarrier.decrement();
			for (Future<?> future : futures) {
				future.get(1, TimeUnit.MINUTES);
			}
		} finally {
			executorService.shutdownNow();
		}
	}
}
