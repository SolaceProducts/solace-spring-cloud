package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.FlowEvent;
import com.solacesystems.jcsmp.FlowEventArgs;
import com.solacesystems.jcsmp.FlowEventHandler;

public class SolaceFlowEventHandler implements FlowEventHandler {
    private XMLMessageMapper xmlMessageMapper;

    /**
     * Use this constructor when an xmlMessageMapper is not yet available at the time the Solace flow is created.
     * setXmlMessageMapper() is then expected to be called.
     */
    public SolaceFlowEventHandler() {
    }

    public SolaceFlowEventHandler(XMLMessageMapper xmlMessageMapper) {
        this.xmlMessageMapper = xmlMessageMapper;
    }

    @Override
    public void handleEvent(Object o, FlowEventArgs flowEventArgs) {
        if (flowEventArgs.getEvent() == FlowEvent.FLOW_RECONNECTED && xmlMessageMapper != null) {
            xmlMessageMapper.resetReadDeliveryCount();
        }
    }

    public void setXmlMessageMapper(XMLMessageMapper mapper) {
        this.xmlMessageMapper = mapper;
    }
}
