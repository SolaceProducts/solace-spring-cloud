package com.solace.spring.cloud.stream.binder.health.indicators;

import com.solace.spring.cloud.stream.binder.properties.SolaceFlowHealthProperties;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowHealthIndicatorTest {

	private FlowHealthIndicator solaceHealthIndicator;

	@BeforeEach
	void setUp() {
		this.solaceHealthIndicator = new FlowHealthIndicator(new SolaceFlowHealthProperties());
	}

	@Test
	void up() {
		this.solaceHealthIndicator.up();
		assertEquals(this.solaceHealthIndicator.health(), Health.up().build());
	}

	@Test
	void reconnecting() {
		this.solaceHealthIndicator.reconnecting(null);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Health.status("RECONNECTING").build().getStatus());
		assertTrue(this.solaceHealthIndicator.getHealth(true).getDetails().isEmpty());
	}

	@Test
	void reconnectingAttempts() {
		SolaceFlowHealthProperties solaceFlowHealthProperties = new SolaceFlowHealthProperties();
		solaceFlowHealthProperties.setReconnectAttemptsUntilDown(10);
		this.solaceHealthIndicator = new FlowHealthIndicator(solaceFlowHealthProperties);
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

	@Test
	void reconnectingWithDetails() {
		FlowEventArgs flowEventArgs = new FlowEventArgs(FlowEvent.FLOW_RECONNECTING, "String_infoStr",
				new Exception("Test Exception"), 500);
		this.solaceHealthIndicator.reconnecting(flowEventArgs);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Health.status("RECONNECTING").build().getStatus());
		assertEquals(this.solaceHealthIndicator.health().getDetails().get("error"), "java.lang.Exception: Test Exception");
		assertEquals(this.solaceHealthIndicator.health().getDetails().get("responseCode"), 500);

	}

	@Test
	void downWithDetails() {
		FlowEventArgs flowEventArgs = new FlowEventArgs(FlowEvent.FLOW_DOWN, "String_infoStr",
				new Exception("Test Exception"), 500);
		this.solaceHealthIndicator.down(flowEventArgs);
		assertEquals(this.solaceHealthIndicator.health().getStatus(), Status.DOWN);
		assertEquals(this.solaceHealthIndicator.health().getDetails().get("error"), "java.lang.Exception: Test Exception");
		assertEquals(this.solaceHealthIndicator.health().getDetails().get("responseCode"), 500);
	}
}