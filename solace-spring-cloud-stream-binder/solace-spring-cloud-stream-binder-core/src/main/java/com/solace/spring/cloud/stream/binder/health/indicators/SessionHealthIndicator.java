package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.health.base.SolaceHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

@NoArgsConstructor
public class SessionHealthIndicator extends SolaceHealthIndicator {
	private final AtomicInteger reconnectCount = new AtomicInteger(0);
	private SolaceSessionHealthProperties solaceHealthSessionProperties;
	private final ReentrantLock writeLock = new ReentrantLock();
	private static final Log logger = LogFactory.getLog(SessionHealthIndicator.class);

	public SessionHealthIndicator(SolaceSessionHealthProperties solaceHealthSessionProperties) {
		this.solaceHealthSessionProperties = solaceHealthSessionProperties;
	}

	public void up() {
		super.healthUp();
		this.reconnectCount.set(0);
	}

	public void reconnecting(@Nullable SessionEventArgs eventArgs) {
		long reconnectAttempt = this.reconnectCount.incrementAndGet();
		if (Optional.of(this.solaceHealthSessionProperties.getReconnectAttemptsUntilDown())
				.filter(maxReconnectAttempts -> maxReconnectAttempts > 0)
				.filter(maxReconnectAttempts -> reconnectAttempt > maxReconnectAttempts)
				.isPresent()) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Solace connection reconnect attempt %s > %s, changing state to down",
						reconnectAttempt, solaceHealthSessionProperties.getReconnectAttemptsUntilDown()));
			}
			this.down(eventArgs);
			this.reconnectCount.set(0);
			return;
		}
		super.healthReconnecting(eventArgs);
	}

	public void down(@Nullable SessionEventArgs eventArgs) {
		super.healthDown(eventArgs);
	}

	@Deprecated
	public void down(@Nullable SessionEventArgs eventArgs, boolean resetReconnectCount) {
		super.healthDown(eventArgs);
	}
}
