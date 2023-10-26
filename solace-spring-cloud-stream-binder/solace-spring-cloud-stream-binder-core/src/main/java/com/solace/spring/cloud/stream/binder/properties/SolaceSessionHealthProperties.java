package com.solace.spring.cloud.stream.binder.properties;

import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("solace.health-check.connection")
public class SolaceSessionHealthProperties {
	/**
	 * <p>The number of session reconnect attempts until the health goes {@code DOWN}. This will happen regardless if
	 * the underlying session is actually still reconnecting. Setting this to {@code 0} will disable this feature.</p>
	 * <p>This feature operates independently of the PubSub+ session reconnect feature. Meaning that if PubSub+
	 * session reconnect is configured to retry less than the value given to this property, then this feature
	 * effectively does nothing.</p>
	 */
	@Min(0)
	@Getter
	@Setter
	private long reconnectAttemptsUntilDown = 0;
}
