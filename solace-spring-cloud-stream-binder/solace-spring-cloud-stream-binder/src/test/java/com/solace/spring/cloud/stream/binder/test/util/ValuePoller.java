package com.solace.spring.cloud.stream.binder.test.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hamcrest.Matcher;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class ValuePoller<T> {
	private final ThrowingSupplier<T> valueSupplier;
	private Matcher<T> until;
	private long timeoutInMillis;
	private long intervalMillis;

	private static final Log logger = LogFactory.getLog(ValuePoller.class);
	private static final int TIMEOUT_MIN = 1;
	private static final int INTERVAL_SEC = 1;

	private ValuePoller(ThrowingSupplier<T> valueSupplier) {
		this.valueSupplier = valueSupplier;
		this.timeoutInMillis = TimeUnit.MINUTES.toMillis(TIMEOUT_MIN);
		this.intervalMillis = TimeUnit.SECONDS.toMillis(INTERVAL_SEC);
	}

	/**
	 * <p>Create default {@link ValuePoller} with defaults:
	 *
	 * <ul>
	 * <li>timeout: {@value TIMEOUT_MIN} min
	 * <li>interval: {@value INTERVAL_SEC} sec
	 * <li>matcher: {@code value != null}
	 * </ul>
	 * @param valueSupplier the supplier that will be called on each poll
	 * @param <T> the type of the value to return and compare against
	 * @return new instance of a {@link ValuePoller}
	 */
	public static <T> ValuePoller<T> poll(ThrowingSupplier<T> valueSupplier) {
		return new ValuePoller<>(valueSupplier);
	}

	/**
	 * <p>Poll until the value satisfies this {@link Matcher}.
	 * <p><b>Default:</b> {@code value != null}
	 * @param matcher the value matcher
	 * @return {@code this}
	 */
	public ValuePoller<T> until(Matcher<T> matcher) {
		this.until = matcher;
		return this;
	}

	/**
	 * <p>Sets the polling timeout
	 * <p><b>Default:</b> {@value TIMEOUT_MIN} min
	 * @param timeoutInMillis timeout in milliseconds
	 * @return {@code this}
	 */
	public ValuePoller<T> withTimeout(long timeoutInMillis) {
		this.timeoutInMillis = timeoutInMillis;
		return this;
	}

	/**
	 * <p>Sets the polling interval
	 * <p><b>Default:</b> {@value INTERVAL_SEC} sec
	 * @param intervalMillis interval in milliseconds
	 * @return {@code this}
	 */
	public ValuePoller<T> withInterval(long intervalMillis) {
		this.intervalMillis = intervalMillis;
		return this;
	}

	/**
	 * Asynchronously executes the poller.
	 * @return a {@link CompletableFuture} that returns the found value or {@code null} if timeout is exceeded
	 */
	public CompletableFuture<T> execute() {
		return CompletableFuture.supplyAsync(() -> {
			T foundValue = null;
			for (long wait = timeoutInMillis, sleep = intervalMillis;
				 (!until.matches(foundValue) || (until == null && wait == timeoutInMillis)) && wait > 0;
				 wait -= sleep, sleep = Math.min(sleep, wait)) {

				logger.info(String.format("Waiting for %s to match matcher, timeout in %s %s", foundValue,
						wait > 999 ? TimeUnit.MILLISECONDS.toSeconds(wait): wait,
						wait > 999 ? "sec" : "ms"));
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					logger.warn("Interrupted, returning null", e);
					return null;
				}
				foundValue = valueSupplier.get();
			}

			return foundValue;
		});
	}
}
