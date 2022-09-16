package com.solace.spring.cloud.stream.binder.meter;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class SolaceMeterAccessorTest {
	@Test
	public void testRecordMessage(@Mock SolaceMessageMeterBinder messageMeterBinder) {
		SolaceMeterAccessor solaceMeterAccessor = new SolaceMeterAccessor(messageMeterBinder);
		String bindingName = "test-binding";
		BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);

		solaceMeterAccessor.recordMessage(bindingName, message);
		Mockito.verify(messageMeterBinder).recordMessage(bindingName, message);
	}
}
