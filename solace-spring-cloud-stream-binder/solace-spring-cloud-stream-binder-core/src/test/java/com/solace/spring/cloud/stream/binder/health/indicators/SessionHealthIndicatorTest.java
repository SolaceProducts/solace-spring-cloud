package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.properties.SolaceSessionHealthProperties;
import com.solacesystems.jcsmp.SessionEventArgs;
import com.solacesystems.jcsmp.impl.SessionEventArgsImpl;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SessionHealthIndicatorTest {
	@Test
	public void testInitialHealth() {
		assertNull(new SessionHealthIndicator(new SolaceSessionHealthProperties()).health());
	}

	@Test
	void testUp() {
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
		healthIndicator.up();
		assertEquals(healthIndicator.health(), Health.up().build());
		assertTrue(healthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@Test
	void testReconnecting() {
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
		healthIndicator.reconnecting(null);
		assertEquals(healthIndicator.health().getStatus(), Health.status("RECONNECTING").build().getStatus());
		assertTrue(healthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@ParameterizedTest(name = "[{index}] reconnectAttemptsUntilDown={0}")
	@ValueSource(longs = {1, 10})
	void testReconnectingDownThresholdReached(long reconnectAttemptsUntilDown, SoftAssertions softly) {
		SolaceSessionHealthProperties properties = new SolaceSessionHealthProperties();
		properties.setReconnectAttemptsUntilDown(reconnectAttemptsUntilDown);
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(properties);
		for (int i = 0; i < reconnectAttemptsUntilDown; i++) {
			healthIndicator.reconnecting(null);
			softly.assertThat(healthIndicator.health()).satisfies(
					h -> assertThat(h.getStatus()).isEqualTo(new Status("RECONNECTING")),
					h -> assertThat(h.getDetails()).isEmpty());
		}

		for (int i = 0; i < 3; i++) {
			healthIndicator.reconnecting(null);
			softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
		}
	}

	@ParameterizedTest(name = "[{index}] resetStatus={0}")
	@ValueSource(strings = {"DOWN", "UP"})
	public void testReconnectingDownThresholdReset(String resetStatus, SoftAssertions softly) {
		SolaceSessionHealthProperties properties = new SolaceSessionHealthProperties();
		properties.setReconnectAttemptsUntilDown(1L);
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(properties);

		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);

		switch (resetStatus) {
			case "DOWN":
				healthIndicator.down(null);
				break;
			case "UP":
				healthIndicator.up();
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + resetStatus);
		}

		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(new Status("RECONNECTING"));
		healthIndicator.reconnecting(null);
		softly.assertThat(healthIndicator.health().getStatus()).isEqualTo(Status.DOWN);
	}

	@Test
	void testDown() {
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
		healthIndicator.down(null);
		assertEquals(healthIndicator.health().getStatus(), Status.DOWN);
		assertTrue(healthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@CartesianTest(name = "[{index}] status={0} withException={1} responseCode={2} info={3}")
	public void testDetails(@CartesianTest.Values(strings = {"DOWN", "RECONNECTING", "UP"}) String status,
							@CartesianTest.Values(booleans = {false, true}) boolean withException,
							@CartesianTest.Values(ints = {-1, 0, 1}) int responseCode,
							@CartesianTest.Values(strings = {"", "some-info"}) String info,
							SoftAssertions softly) {
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
		Exception healthException = withException ? new Exception("test") : null;
		SessionEventArgs sessionEventArgs = new SessionEventArgsImpl(null, info, healthException, responseCode);
		switch (status) {
			case "DOWN":
				healthIndicator.down(sessionEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(sessionEventArgs);
				break;
			case "UP":
				healthIndicator.up();
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.health();

		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));

		if (withException && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.isNotEmpty()
					.extractingByKey("error")
					.isEqualTo(healthException.getClass().getName() + ": " + healthException.getMessage());
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("error");
		}

		if (responseCode != 0 && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.extractingByKey("responseCode")
					.isEqualTo(responseCode);
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("responseCode");
		}

		if (!info.isEmpty() && !status.equals("UP")) {
			softly.assertThat(health.getDetails())
					.extractingByKey("info")
					.isEqualTo(info);
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("info");
		}
	}

	@ParameterizedTest(name = "[{index}] status={0}")
	@ValueSource(strings = {"DOWN", "RECONNECTING", "UP"})
	public void testWithoutDetails(String status, SoftAssertions softly) {
		SessionHealthIndicator healthIndicator = new SessionHealthIndicator(new SolaceSessionHealthProperties());
		SessionEventArgs sessionEventArgs = new SessionEventArgsImpl(null, "some-info", new RuntimeException("test"), 1);
		switch (status) {
			case "DOWN":
				healthIndicator.down(sessionEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(sessionEventArgs);
				break;
			case "UP":
				healthIndicator.up();
				break;
			default:
				throw new IllegalArgumentException("Test error: No handling for status=" + status);
		}
		Health health = healthIndicator.getHealth(false);
		softly.assertThat(health.getStatus()).isEqualTo(new Status(status));
		softly.assertThat(health.getDetails()).isEmpty();
	}
}