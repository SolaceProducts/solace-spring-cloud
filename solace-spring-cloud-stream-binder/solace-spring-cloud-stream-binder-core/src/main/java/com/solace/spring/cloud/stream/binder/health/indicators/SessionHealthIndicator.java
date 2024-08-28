package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.health.base.SolaceHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class SessionHealthIndicator extends SolaceHealthIndicator {
	private final AtomicInteger reconnectCount = new AtomicInteger(0);
	private final SolaceSessionHealthProperties solaceHealthSessionProperties;
	private final ReentrantLock writeLock = new ReentrantLock();
	private static final Logger LOGGER = LoggerFactory.getLogger(SessionHealthIndicator.class);

	public SessionHealthIndicator(SolaceSessionHealthProperties solaceHealthSessionProperties) {
		this.solaceHealthSessionProperties = solaceHealthSessionProperties;
	}

	public void up() {
		writeLock.lock();
		try {
			LOGGER.trace("Reset reconnect count");
			this.reconnectCount.set(0);
			super.healthUp();
		} finally {
			writeLock.unlock();
		}
	}

	public void reconnecting(@Nullable SessionEventArgs eventArgs) {
		writeLock.lock();
		try {
			long reconnectAttempt = this.reconnectCount.incrementAndGet();
			if (Optional.of(this.solaceHealthSessionProperties.getReconnectAttemptsUntilDown())
					.filter(maxReconnectAttempts -> maxReconnectAttempts > 0)
					.filter(maxReconnectAttempts -> reconnectAttempt > maxReconnectAttempts)
					.isPresent()) {
				LOGGER.debug("Solace connection reconnect attempt {} > {}, changing state to down",
						reconnectAttempt, solaceHealthSessionProperties.getReconnectAttemptsUntilDown());
				this.down(eventArgs, false);
				return;
			}

			super.healthReconnecting(eventArgs);
		} finally {
			writeLock.unlock();
		}
	}

	public void down(@Nullable SessionEventArgs eventArgs) {
		down(eventArgs, true);
	}

	public void down(@Nullable SessionEventArgs eventArgs, boolean resetReconnectCount) {
		writeLock.lock();
		try {
			if (resetReconnectCount) {
				LOGGER.trace("Reset reconnect count");
				this.reconnectCount.set(0);
			}
			super.healthDown(eventArgs);
		} finally {
			writeLock.unlock();
		}
	}
}
