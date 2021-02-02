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

	public void reset() {
		if (Long.compareUnsigned(counter.getAndSet(0), 0) != 0) {
			awaitLock.lock();
			try {
				isZero.signalAll();
			} finally {
				awaitLock.unlock();
			}
		}
	}

	/**
	 * Wait until counter is zero.
	 * @param timeout the maximum wait time. If less than 0, then wait forever.
	 * @param unit the timeout unit.
	 * @return true if counter reached zero. False if timed out.
	 * @throws InterruptedException if the wait was interrupted
	 */
	public boolean awaitEmpty(long timeout, TimeUnit unit) throws InterruptedException {
		awaitLock.lock();
		try {
			if (timeout > 0) {
				logger.info(String.format("Waiting for %s items, time remaining: %s %s", counter.get(), timeout, unit));
				long expiry = unit.toMillis(timeout) + System.currentTimeMillis();
				while (isGreaterThanZero()) {
					long realTimeout = expiry - System.currentTimeMillis();
					if (realTimeout <= 0) {
						return false;
					}
					isZero.await(realTimeout, TimeUnit.MILLISECONDS);
				}
				return true;
			} else if (timeout < 0) {
				while (isGreaterThanZero()) {
					logger.info(String.format("Waiting for %s items", counter.get()));
					isZero.await(5, TimeUnit.SECONDS);
				}
				return true;
			} else {
				return !isGreaterThanZero();
			}
		} finally {
			awaitLock.unlock();
		}
	}

	private boolean isGreaterThanZero() {
		return Long.compareUnsigned(counter.get(), 0) > 0;
	}

	/**
	 * Get the unsigned count.
	 * @return The count.
	 */
	public long getCount() {
		return counter.get();
	}
}
