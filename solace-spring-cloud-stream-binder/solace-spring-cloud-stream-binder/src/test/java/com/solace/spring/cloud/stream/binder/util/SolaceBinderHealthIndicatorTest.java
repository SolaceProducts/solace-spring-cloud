package com.solace.spring.cloud.stream.binder.util;

import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SolaceBinderHealthIndicatorTest {
	@Test
	public void testInitialHealth() {
		assertNull(new SolaceBinderHealthIndicator().health());
	}

	@Test
	public void testUp(SoftAssertions softly) {
		SolaceBinderHealthIndicator healthIndicator = new SolaceBinderHealthIndicator();
		healthIndicator.up();
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.UP);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testDown(SoftAssertions softly) {
		SolaceBinderHealthIndicator healthIndicator = new SolaceBinderHealthIndicator();
		healthIndicator.down(null, 0, null);
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@CartesianTest(name = "[{index}] responseCode={0} info={1}")
	public void testDownDetails(@Values(ints = {-1, 0, 1}) int responseCode,
								@Values(strings = {"", "some-info"}) String info,
								SoftAssertions softly) {
		SolaceBinderHealthIndicator healthIndicator = new SolaceBinderHealthIndicator();
		Exception healthException = new Exception("test");
		healthIndicator.down(healthException, responseCode, info);
		Health health = healthIndicator.health();

		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails())
				.isNotEmpty()
				.hasEntrySatisfying("error", error -> assertThat(error)
						.isEqualTo(healthException.getClass().getName() + ": " + healthException.getMessage()));

		if (responseCode != 0) {
			softly.assertThat(health.getDetails())
					.hasEntrySatisfying("responseCode", r -> assertThat(r).isEqualTo(responseCode));
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("responseCode");
		}

		if (!info.isEmpty()) {
			softly.assertThat(health.getDetails())
					.hasEntrySatisfying("info", i -> assertThat(i).isEqualTo(info));
		} else {
			softly.assertThat(health.getDetails()).doesNotContainKey("info");
		}
	}

	@Test
	public void testDownWithoutDetails(SoftAssertions softly) {
		SolaceBinderHealthIndicator healthIndicator = new SolaceBinderHealthIndicator();
		healthIndicator.down(new RuntimeException("test"), 1, "some-info");
		Health health = healthIndicator.getHealth(false);
		softly.assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		softly.assertThat(health.getDetails()).isEmpty();
	}

	@Test
	public void testReconnecting(SoftAssertions softly) {
		SolaceBinderHealthIndicator healthIndicator = new SolaceBinderHealthIndicator();
		healthIndicator.reconnecting();
		Health health = healthIndicator.health();
		softly.assertThat(health.getStatus()).isEqualTo(new Status("RECONNECTING"));
		softly.assertThat(health.getDetails()).isEmpty();
	}
}
