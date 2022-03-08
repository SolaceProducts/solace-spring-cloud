package com.solace.spring.cloud.stream.binder.util;

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
	public void testConnected(@Mock SolaceBinderHealthIndicator healthIndicator) {
		SolaceSessionEventHandler sessionEventHandler = new SolaceSessionEventHandler(healthIndicator);
		sessionEventHandler.connected();
		Mockito.verify(healthIndicator, Mockito.times(1)).up();
	}

	@ParameterizedTest
	@EnumSource(SessionEvent.class)
	public void testHandleEvent(SessionEvent event,
								@Mock SessionEventArgs eventArgs,
								@Mock SolaceBinderHealthIndicator healthIndicator) {
		Exception exception = new Exception("test");
		String info = "test=info";
		int responseCode = 0;
		Mockito.when(eventArgs.getEvent()).thenReturn(event);
		if (event.equals(SessionEvent.DOWN_ERROR)) {
			Mockito.when(eventArgs.getException()).thenReturn(exception);
			Mockito.when(eventArgs.getInfo()).thenReturn(info);
			Mockito.when(eventArgs.getResponseCode()).thenReturn(responseCode);
		}

		SolaceSessionEventHandler sessionEventHandler = new SolaceSessionEventHandler(healthIndicator);
		sessionEventHandler.handleEvent(eventArgs);

		switch (event) {
			case DOWN_ERROR:
				Mockito.verify(healthIndicator, Mockito.times(1))
						.down(exception, responseCode, info);
				break;
			case RECONNECTING:
				Mockito.verify(healthIndicator, Mockito.times(1)).reconnecting();
				break;
			case RECONNECTED:
				Mockito.verify(healthIndicator, Mockito.times(1)).up();
				break;
			default:
				Mockito.verifyNoInteractions(healthIndicator);
		}
	}
}
