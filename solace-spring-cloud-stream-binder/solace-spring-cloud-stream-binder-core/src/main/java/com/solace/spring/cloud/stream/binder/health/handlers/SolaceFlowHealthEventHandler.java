package com.solace.spring.cloud.stream.binder.health.handlers;

import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.util.SolaceFlowEventHandler;
import com.solace.spring.cloud.stream.binder.util.XMLMessageMapper;
import com.solacesystems.jcsmp.FlowEventArgs;

public class SolaceFlowHealthEventHandler extends SolaceFlowEventHandler {
	private final FlowHealthIndicator flowHealthIndicator;

	public SolaceFlowHealthEventHandler(XMLMessageMapper xmlMessageMapper,
	                                    String flowReceiverContainerId,
	                                    FlowHealthIndicator flowHealthIndicator) {
		super(xmlMessageMapper, flowReceiverContainerId);
		this.flowHealthIndicator = flowHealthIndicator;
	}

	@Override
	public void handleEvent(Object source, FlowEventArgs flowEventArgs) {
		super.handleEvent(source, flowEventArgs);

		if (flowEventArgs.getEvent() != null) {
			switch (flowEventArgs.getEvent()) {
				case FLOW_DOWN:
					flowHealthIndicator.down(flowEventArgs);
					break;
				case FLOW_RECONNECTING:
					flowHealthIndicator.reconnecting(flowEventArgs);
					break;
				case FLOW_UP:
				case FLOW_RECONNECTED:
					flowHealthIndicator.up();
					break;
			}
		}
	}

	/**
	 * This method is used only on during  event of binding to source
	 */
	public void setHealthStatusUp() {
		this.flowHealthIndicator.up();
	}
}
