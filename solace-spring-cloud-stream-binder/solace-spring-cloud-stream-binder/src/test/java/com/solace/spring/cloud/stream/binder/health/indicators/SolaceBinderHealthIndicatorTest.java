package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

public class SolaceBinderHealthIndicatorTest {
	private SessionHealthIndicator healthIndicator;

	@BeforeEach
	void BeforeEach(){
		SolaceSessionHealthProperties solaceSessionHealthProperties = new SolaceSessionHealthProperties();
		solaceSessionHealthProperties.setReconnectAttemptsUntilDown(10);
		this.healthIndicator = new SessionHealthIndicator(solaceSessionHealthProperties);
	}

	@Test
	public void testUp(SoftAssertions softly) {
		healthIndicator.up();
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		healthIndicator.down(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		healthIndicator.reconnecting(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(new Status("RECONNECTING"));
		softly.assertThat(health.getDetails()).isEmpty();
	}
}
