package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SolaceFlowEventHandler implements FlowEventHandler {

    private static final Log logger = LogFactory.getLog(SolaceFlowEventHandler.class);
    private final XMLMessageMapper xmlMessageMapper;
    private final String flowReceiverContainerId;

    public SolaceFlowEventHandler(XMLMessageMapper xmlMessageMapper, String flowReceiverContainerId) {
        this.xmlMessageMapper = xmlMessageMapper;
        this.flowReceiverContainerId = flowReceiverContainerId;
    }

    @Override
    public void handleEvent(Object o, FlowEventArgs flowEventArgs) {
        if (flowEventArgs.getEvent() == FlowEvent.FLOW_RECONNECTED && xmlMessageMapper != null) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Received flow event %s for flow receiver container %s. Will clear ignored properties.",
                        flowEventArgs.getEvent().name(), flowReceiverContainerId));
            }
            xmlMessageMapper.resetIgnoredProperties(flowReceiverContainerId);
        }
    }

}