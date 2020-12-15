package com.solace.spring.cloud.stream.binder.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class UnsignedCounterBarrier {
	private final AtomicLong counter; // Treated as an unsigned long (i.e. range from 0 -> -1)
	private final Lock awaitLock = new ReentrantLock();
	private final Condition isZero = awaitLock.newCondition();

	private static final Log logger = LogFactory.getLog(UnsignedCounterBarrier.class);

	public UnsignedCounterBarrier(long initialValue) {
		counter = new AtomicLong(initialValue);
	}

	public UnsignedCounterBarrier() {
		this(0);
	}

	public void increment() {
		// Assuming we won't ever increment past -1
		counter.updateAndGet(c -> Long.compareUnsigned(c, -1) < 0 ? c + 1 : c);
	}

	public void decrement() {
		// Assuming we won't ever decrement below 0
		if (counter.updateAndGet(c -> Long.compareUnsigned(c, 0) > 0 ? c - 1 : c) == 0) {
			awaitLock.lock();
			try {
				isZero.signalAll();
			} finally {
				awaitLock.unlock();
			}
		}
	}

	public void awaitEmpty() throws InterruptedException {
		awaitLock.lock();
		try {
			while (Long.compareUnsigned(counter.get(), 0) > 0) {
				logger.info(String.format("Waiting for %s items", counter.get()));
				isZero.await(5, TimeUnit.SECONDS);
			}
		} finally {
			awaitLock.unlock();
		}
	}

	/**
	 * Get the unsigned count.
	 * @return The count.
	 */
	public long getCount() {
		return counter.get();
	}
}
