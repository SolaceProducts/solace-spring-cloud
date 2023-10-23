package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solacesystems.jcsmp.FlowEventArgs;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;

class FlowHealthIndicatorTest {

	@Test
	public void testInitialHealth() {
		assertThat(new FlowHealthIndicator().health()).isNull();
	}

	@Test
	public void testUp(SoftAssertions softly) {
		FlowHealthIndicator healthIndicator = new FlowHealthIndicator();
		healthIndicator.up();
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		FlowHealthIndicator healthIndicator = new FlowHealthIndicator();
		healthIndicator.down(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		FlowHealthIndicator healthIndicator = new FlowHealthIndicator();
		healthIndicator.reconnecting(null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(new Status("RECONNECTING"));
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@CartesianTest(name = "[{index}] status={0} responseCode={1} info={2}")
	public void testDetails(@CartesianTest.Values(strings = {"DOWN", "RECONNECTING", "UP"}) String status,
							@CartesianTest.Values(booleans = {false, true}) boolean withException,
							@CartesianTest.Values(ints = {-1, 0, 1}) int responseCode,
							@CartesianTest.Values(strings = {"", "some-info"}) String info,
							SoftAssertions softly) {
		FlowHealthIndicator healthIndicator = new FlowHealthIndicator();
		Exception healthException = withException ? new Exception("test") : null;
		FlowEventArgs flowEventArgs = new FlowEventArgs(null, info, healthException, responseCode);
		switch (status) {
			case "DOWN":
				healthIndicator.down(flowEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(flowEventArgs);
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
		FlowHealthIndicator healthIndicator = new FlowHealthIndicator();
		FlowEventArgs flowEventArgs = new FlowEventArgs(null, "some-info", new RuntimeException("test"), 1);
		switch (status) {
			case "DOWN":
				healthIndicator.down(flowEventArgs);
				break;
			case "RECONNECTING":
				healthIndicator.reconnecting(flowEventArgs);
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