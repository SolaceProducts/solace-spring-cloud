package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.health.base.SolaceHealthIndicator;
import com.solace.spring.cloud.stream.binder.properties.SolaceFlowHealthProperties;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.lang.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class FlowHealthIndicator extends SolaceHealthIndicator {
	private final SolaceFlowHealthProperties solaceFlowHealthProperties;
	private final AtomicLong reconnectCount = new AtomicLong(0);
	private static final Log logger = LogFactory.getLog(FlowHealthIndicator.class);

	public FlowHealthIndicator(SolaceFlowHealthProperties solaceFlowHealthProperties) {
		this.solaceFlowHealthProperties = solaceFlowHealthProperties;
	}

	public void up() {
		super.healthUp();
		this.reconnectCount.set(0);
	}

	public void reconnecting(@Nullable FlowEventArgs eventArgs) {
		long reconnectAttempt = this.reconnectCount.incrementAndGet();
		if (Optional.of(this.solaceFlowHealthProperties.getReconnectAttemptsUntilDown())
				.filter(maxReconnectAttempts -> maxReconnectAttempts > 0)
				.filter(maxReconnectAttempts -> reconnectAttempt > maxReconnectAttempts)
				.isPresent()) {
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Solace flow reconnect attempt %s > %s, changing state to down",
						reconnectAttempt, this.solaceFlowHealthProperties.getReconnectAttemptsUntilDown()));
			}
			this.down(eventArgs);
			return;
		}
		super.healthReconnecting(eventArgs);
	}

	public void down(@Nullable FlowEventArgs eventArgs) {
		super.healthDown(eventArgs);
		this.reconnectCount.set(0);
	}

	@Deprecated
	public void down(@Nullable FlowEventArgs eventArgs, boolean resetReconnectCount) {
		super.healthDown(eventArgs);
	}
}
