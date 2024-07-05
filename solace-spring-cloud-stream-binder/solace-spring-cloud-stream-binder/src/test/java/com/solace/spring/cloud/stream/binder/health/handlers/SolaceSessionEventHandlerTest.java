package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.SessionEvent;
import com.solacesystems.jcsmp.SessionEventArgs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SolaceSessionEventHandlerTest {
	@Test
	public void testConnected(@Mock SessionHealthIndicator healthIndicator) {
		SolaceSessionEventHandler sessionEventHandler = new SolaceSessionEventHandler(new JCSMPProperties(), null, healthIndicator);
		sessionEventHandler.setSessionHealthUp();
		Mockito.verify(healthIndicator, Mockito.times(1)).up();
		Mockito.verifyNoMoreInteractions(healthIndicator);
	}

	@ParameterizedTest
	@EnumSource(SessionEvent.class)
	public void testHandleEvent(SessionEvent event,
	                            @Mock SessionEventArgs eventArgs,
	                            @Mock SessionHealthIndicator healthIndicator) {
		Mockito.when(eventArgs.getEvent()).thenReturn(event);

		SolaceSessionEventHandler sessionEventHandler = new SolaceSessionEventHandler(new JCSMPProperties(), null, healthIndicator);
		sessionEventHandler.handleEvent(eventArgs);

		switch (event) {
			case DOWN_ERROR -> Mockito.verify(healthIndicator, Mockito.times(1)).down(eventArgs);
			case RECONNECTING -> Mockito.verify(healthIndicator, Mockito.times(1)).reconnecting(eventArgs);
			case RECONNECTED -> Mockito.verify(healthIndicator, Mockito.times(1)).up();
		}
	}
}
