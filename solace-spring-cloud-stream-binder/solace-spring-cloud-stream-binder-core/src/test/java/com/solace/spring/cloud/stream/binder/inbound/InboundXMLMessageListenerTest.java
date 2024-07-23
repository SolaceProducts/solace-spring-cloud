package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.UnboundFlowReceiverContainerException;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.StaleSessionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@ExtendWith(OutputCaptureExtension.class)
public class InboundXMLMessageListenerTest {

    @Mock
    private ConsumerDestination consumerDestination;

    @Test
    public void testListenerIsStoppedOnStaleSessionException(@Mock FlowReceiverContainer flowReceiverContainer, CapturedOutput output)
            throws UnboundFlowReceiverContainerException, JCSMPException {

        when(flowReceiverContainer.receive(Mockito.anyInt()))
                .thenThrow(new StaleSessionException("Session has become stale", new JCSMPException("Specific JCSMP exception")));

        BasicInboundXMLMessageListener inboundXMLMessageListener = new BasicInboundXMLMessageListener(
                flowReceiverContainer, consumerDestination, null, null, null, null, null, null,
                null, null, false);

        inboundXMLMessageListener.run();

        assertThat(output)
                .contains("Session has lost connection")
                .contains("Closing flow receiver to destination");

        verify(flowReceiverContainer).unbind();
    }
}
