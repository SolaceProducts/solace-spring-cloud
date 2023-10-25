package com.solace.spring.cloud.stream.binder.health.base;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.lang.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

@NoArgsConstructor
public class SolaceHealthIndicator implements HealthIndicator {
	private static final String STATUS_RECONNECTING = "RECONNECTING";
	private static final String INFO = "info";
	private static final String RESPONSE_CODE = "responseCode";
	@Setter(AccessLevel.PACKAGE)
	private volatile Health health;
	private static final Log logger = LogFactory.getLog(SolaceHealthIndicator.class);

	private static void logDebugStatus(String status) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Solace connection/flow status is %s", status));
		}
	}
	protected void healthUp() {
			health = Health.up().build();
			logDebugStatus(String.valueOf(Status.UP));
	}
	protected <T> void healthReconnecting(@Nullable T eventArgs) {
			health = addEventDetails(Health.status(STATUS_RECONNECTING), eventArgs).build();
			logDebugStatus(STATUS_RECONNECTING);
	}

	protected <T> void healthDown(@Nullable T eventArgs) {
			health = addEventDetails(Health.down(), eventArgs).build();
			logDebugStatus(String.valueOf(Status.DOWN));
	}

	public <T> Health.Builder addEventDetails(Health.Builder builder, @Nullable T eventArgs) {
		if (eventArgs == null) {
			return builder;
		}

		try {
			Optional.ofNullable(eventArgs.getClass().getMethod("getException").invoke(eventArgs))
					.ifPresent(ex -> builder.withException((Throwable) ex));
			Optional.of(eventArgs.getClass().getMethod("getResponseCode").invoke(eventArgs))
					.filter(c -> ((int) c) != 0)
					.ifPresent(c -> builder.withDetail(RESPONSE_CODE, c));
			Optional.ofNullable(eventArgs.getClass().getMethod("getInfo").invoke(eventArgs))
					.filter(t -> StringUtils.isNotBlank(String.valueOf(t)))
					.ifPresent(info -> builder.withDetail(INFO, info));
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException(e);
		}

		return builder;
	}

	@Override
	public Health health() {
		return health;
	}
}