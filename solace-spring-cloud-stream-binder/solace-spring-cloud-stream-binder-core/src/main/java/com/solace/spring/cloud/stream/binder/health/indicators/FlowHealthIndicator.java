package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.health.base.SolaceHealthIndicator;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.springframework.lang.Nullable;

public class FlowHealthIndicator extends SolaceHealthIndicator {
	public void up() {
		super.healthUp();
	}

	public void reconnecting(@Nullable FlowEventArgs eventArgs) {
		super.healthReconnecting(eventArgs);
	}

	public void down(@Nullable FlowEventArgs eventArgs) {
		super.healthDown(eventArgs);
	}
}
