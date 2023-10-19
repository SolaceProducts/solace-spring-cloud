package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHealthIndicatorTest {

	private SessionHealthIndicator solaceHealthIndicator;

	@BeforeEach
	void setUp() {
		this.solaceHealthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
	}

	@Test
	void up() {
		this.solaceHealthIndicator.up();
		assertEquals(this.solaceHealthIndicator.health(), Health.up().build());
		assertTrue(this.solaceHealthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@Test
	void reconnecting() {
		this.solaceHealthIndicator.reconnecting(null);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Health.status("RECONNECTING").build().getStatus());
		assertTrue(this.solaceHealthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@Test
	void reconnectingAttempts() {
		SolaceSessionHealthProperties solaceSessionHealthProperties = new SolaceSessionHealthProperties();
		solaceSessionHealthProperties.setReconnectAttemptsUntilDown(10);
		this.solaceHealthIndicator = new SessionHealthIndicator(solaceSessionHealthProperties);
		this.solaceHealthIndicator.reconnecting(null);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Health.status("RECONNECTING").build().getStatus());
		assertTrue(this.solaceHealthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@Test
	void down() {
		this.solaceHealthIndicator.down(null);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Status.DOWN);
		assertTrue(this.solaceHealthIndicator.getHealth(true).getDetails().isEmpty());
	}
}