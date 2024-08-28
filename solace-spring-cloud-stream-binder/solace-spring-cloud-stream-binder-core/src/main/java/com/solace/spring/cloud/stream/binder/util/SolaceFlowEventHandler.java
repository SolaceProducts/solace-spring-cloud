package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolaceFlowEventHandler implements FlowEventHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceFlowEventHandler.class);
	private final XMLMessageMapper xmlMessageMapper;
	private final String flowReceiverContainerId;

	public SolaceFlowEventHandler(XMLMessageMapper xmlMessageMapper, String flowReceiverContainerId) {
		this.xmlMessageMapper = xmlMessageMapper;
		this.flowReceiverContainerId = flowReceiverContainerId;
	}

	@Override
	public void handleEvent(Object source, FlowEventArgs flowEventArgs) {
		LOGGER.debug("({}): Received Solace Flow event [{}].", source, flowEventArgs);

		if (flowEventArgs.getEvent() == FlowEvent.FLOW_RECONNECTED && xmlMessageMapper != null) {
			LOGGER.debug("Received flow event {} for flow receiver container {}. Will clear ignored properties.",
					flowEventArgs.getEvent().name(), flowReceiverContainerId);
			xmlMessageMapper.resetIgnoredProperties(flowReceiverContainerId);
		}
	}

}
