package com.solace.spring.cloud.stream.binder.meter;

import com.solacesystems.jcsmp.XMLMessage;

public class SolaceMeterAccessor {
	private final SolaceMessageMeterBinder solaceMessageMeterBinder;

	public SolaceMeterAccessor(SolaceMessageMeterBinder solaceMessageMeterBinder) {
		this.solaceMessageMeterBinder = solaceMessageMeterBinder;
	}

	public void recordMessage(String bindingName, XMLMessage message) {
		solaceMessageMeterBinder.recordMessage(bindingName, message);
	}
}
