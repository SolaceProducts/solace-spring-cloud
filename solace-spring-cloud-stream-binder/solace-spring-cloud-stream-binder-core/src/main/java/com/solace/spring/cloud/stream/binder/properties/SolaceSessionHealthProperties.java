package com.solace.spring.cloud.stream.binder.properties;

import jakarta.validation.constraints.Min;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("solace.health-check.connection")
public class SolaceSessionHealthProperties {
	@Min(0)
	@Getter
	@Setter
	private long reconnectAttemptsUntilDown = 0;
}
