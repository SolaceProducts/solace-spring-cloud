package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolaceFlowHealthEventHandler implements FlowEventHandler {
	private final FlowHealthIndicator flowHealthIndicator;
	private final XMLMessageMapper xmlMessageMapper;
	private final String flowReceiverContainerId;
	private static final Log logger = LogFactory.getLog(SolaceFlowHealthEventHandler.class);

	public SolaceFlowHealthEventHandler(XMLMessageMapper xmlMessageMapper,
	                                    String flowReceiverContainerId,
	                                    FlowHealthIndicator flowHealthIndicator) {
		this.flowHealthIndicator = flowHealthIndicator;
		this.xmlMessageMapper = xmlMessageMapper;
		this.flowReceiverContainerId = flowReceiverContainerId;
	}

	@Override
	public void handleEvent(Object source, FlowEventArgs flowEventArgs) {
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("(%s): Received Solace Flow event [%s].", source, flowEventArgs));
		}
		if (flowEventArgs.getEvent() != null) {
			switch (flowEventArgs.getEvent()) {
				case FLOW_DOWN:
					flowHealthIndicator.down(flowEventArgs);
					break;
				case FLOW_RECONNECTING:
					resetXmlMessageMapperProperties(flowEventArgs);
					flowHealthIndicator.reconnecting(flowEventArgs);
					break;
				case FLOW_UP:
				case FLOW_RECONNECTED:
					flowHealthIndicator.up();
					break;
			}
		}
	}

	private void resetXmlMessageMapperProperties(FlowEventArgs flowEventArgs) {
		if(xmlMessageMapper != null){
			if (logger.isDebugEnabled()) {
				logger.debug(String.format("Received flow event %s for flow receiver container %s. Will clear ignored properties.",
						flowEventArgs.getEvent().name(), flowReceiverContainerId));
			}
			xmlMessageMapper.resetIgnoredProperties(flowReceiverContainerId);
		}
	}

	/**
	 * This method is used only on during  event of binding to source
	 */
	public void setHealthStatusUp() {
		this.flowHealthIndicator.up();
	}
}
