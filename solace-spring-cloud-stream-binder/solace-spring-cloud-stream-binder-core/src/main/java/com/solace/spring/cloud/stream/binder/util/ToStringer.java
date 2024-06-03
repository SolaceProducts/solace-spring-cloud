package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;

/**
 * To string utility for 3'rd party classes that do not override toString in a meaningful way, to be used for logging or debugging purposes
 */
public final class ToStringer {

    /**
     * provides a simplified toString for {@link EndpointProperties}
     * @param ep to tbe turned to String
     * @return string representation of the object
     */
    public static String toString(EndpointProperties ep) {
        if (ep== null)
            return "EndpointProperties{NULL}";
        return "EndpointProperties{" +
                "mAccessType=" + ep.getAccessType() +
                ", mMaxMsgSize=" + ep.getMaxMsgSize() +
                ", mPermission=" + ep.getPermission() +
                '}';
    }

    /**
     * provides a simplified toString for {@link Endpoint}
     * @param e to tbe turned to String
     * @return string representation of the object
     */
    public static String toString(Endpoint e) {
        if (e==null)
            return "Endpoint{NULL}";
        return "Endpoint{" +
                " class:" + e.getClass()+
                " _name='" + e.getName() + '\'' +
                ", _durable=" + e.isDurable() +
                '}';
    }

    /**
     * provides a simplified toString for {@link ConsumerFlowProperties}
     * @param cfp to tbe turned to String
     * @return string representation of the object
     */
    public static String toString(ConsumerFlowProperties cfp) {

        return "ConsumerFlowProperties{" +
                "endpoint=" + toString(cfp.getEndpoint()) +
                ", newSubscription=" + cfp.getNewSubscription() +
                ", sqlSelector='" + cfp.getSelector() + '\'' +
                ", startState=" + cfp.isStartState() +
                ", noLocal=" + cfp.isNoLocal() +
                ", activeFlowIndication=" + cfp.isActiveFlowIndication() +
                ", ackMode='" + cfp.getAckMode() + '\'' +
                ", windowSize=" + cfp.getTransportWindowSize() +
                ", ackTimerMs=" + cfp.getAckTimerInMsecs() +
                ", ackThreshold=" + cfp.getAckThreshold() +
                ", reconnectTries=" + cfp.getReconnectTries() +
                ", outcomes=" + cfp.getRequiredSettlementOutcomes() +
                ", flowSessionProps=" + cfp.getFlowSessionProps() +
                ", segmentFlow=" + cfp.isSegmentFlow() +
                '}';
    }

}
