package com.solace.spring.cloud.stream.binder.outbound;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.util.CorrelationData;
import com.solace.spring.cloud.stream.binder.util.ErrorChannelSendingCorrelationKey;
import com.solace.spring.cloud.stream.binder.util.JCSMPSessionProducerManager;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageHeaderErrorMessageStrategy;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.provisioning.ProducerDestination;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10)
@ExtendWith(MockitoExtension.class)
public class JCSMPOutboundMessageHandlerTest {

	private JCSMPOutboundMessageHandler messageHandler;
	private JCSMPStreamingPublishCorrelatingEventHandler pubEventHandler;
	private ArgumentCaptor<XMLMessage> xmlMessageCaptor;
	private ExtendedProducerProperties<SolaceProducerProperties> producerProperties;
	@Mock private XMLMessageProducer messageProducer;
	@Mock private SolaceMeterAccessor solaceMeterAccessor;

	@BeforeEach
	public void init(@Mock JCSMPSession session,
					 @Mock MessageChannel errChannel,
					 @Mock SolaceMessageHeaderErrorMessageStrategy errorMessageStrategy) throws JCSMPException {
		xmlMessageCaptor = ArgumentCaptor.forClass(XMLMessage.class);

		ArgumentCaptor<JCSMPStreamingPublishCorrelatingEventHandler> pubEventHandlerCaptor = ArgumentCaptor
				.forClass(JCSMPStreamingPublishCorrelatingEventHandler.class);
		Mockito.when(session.getMessageProducer(pubEventHandlerCaptor.capture())).thenReturn(messageProducer);

		ProducerDestination dest = Mockito.mock(ProducerDestination.class);
		Mockito.when(dest.getName()).thenReturn("fake/topic");

		producerProperties = new ExtendedProducerProperties<>(new SolaceProducerProperties());
		producerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(100));

		messageHandler = new JCSMPOutboundMessageHandler(
				dest,
				session,
				errChannel,
				new JCSMPSessionProducerManager(session),
				producerProperties,
				solaceMeterAccessor
		);
		messageHandler.setErrorMessageStrategy(errorMessageStrategy);
		messageHandler.start();

		pubEventHandler = pubEventHandlerCaptor.getValue();
	}

	@Test()
	public void test_responseReceived_withInTimeout() throws Exception {
		CorrelationData correlationData = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationData));

		pubEventHandler.responseReceivedEx(getCorrelationKey());

		correlationData.getFuture().get(100, TimeUnit.MILLISECONDS);
	}

	@Test()
	public void test_handleError_withInTimeout() {
		CorrelationData correlationData = new CorrelationData();
		Message<String> msg = getMessage(correlationData);
		messageHandler.handleMessage(msg);

		pubEventHandler.handleErrorEx(createCorrelationKey(correlationData, msg), new JCSMPException("ooooops"), 1111);

		ExecutionException exception = assertThrows(ExecutionException.class, () -> correlationData.getFuture().get(100, TimeUnit.MILLISECONDS));
		assertNotNull(exception);
		assertTrue(exception.getCause() instanceof MessagingException);
		assertTrue(exception.getCause().getCause() instanceof JCSMPException);
		assertEquals("ooooops", exception.getCause().getCause().getMessage());
	}

	@Test()
	public void test_responseReceived_withOutTimeout() {
		CorrelationData correlationData = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationData));

		assertThrows(TimeoutException.class, () -> correlationData.getFuture().get(100, TimeUnit.MILLISECONDS));
	}

	@Test()
	public void test_responseReceived_raceCondition() throws ExecutionException, InterruptedException, TimeoutException {
		CorrelationData correlationDataA = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationDataA));
		CorrelationData correlationDataB = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationDataB));
		CorrelationData correlationDataC = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationDataC));

		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataB));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataA));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataC));

		correlationDataA.getFuture().get(100, TimeUnit.MILLISECONDS);
		correlationDataB.getFuture().get(100, TimeUnit.MILLISECONDS);
		correlationDataC.getFuture().get(100, TimeUnit.MILLISECONDS);
	}

	@Test()
	public void test_responseReceived_messageIdCollision_oneAfterTheOther() throws ExecutionException, InterruptedException, TimeoutException {
		CorrelationData correlationDataA = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationDataA));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataA));

		correlationDataA.getFuture().get(100, TimeUnit.MILLISECONDS);


		CorrelationData correlationDataB = new CorrelationData();
		messageHandler.handleMessage(getMessage(correlationDataB));
		pubEventHandler.responseReceivedEx(createCorrelationKey(correlationDataB));

		correlationDataB.getFuture().get(100, TimeUnit.MILLISECONDS);
	}

	@ParameterizedTest(name = "[{index}] success={0}")
	@ValueSource(booleans = {false, true})
	public void testMeter(boolean success) throws Exception {
		Message<String> message = MessageBuilder.withPayload(RandomStringUtils.randomAlphanumeric(100))
				.build();

		if (success) {
			messageHandler.handleMessage(message);
		} else {
			JCSMPException exception = new JCSMPException("Expected exception");
			Mockito.doThrow(exception)
					.when(messageProducer)
					.send(xmlMessageCaptor.capture(), Mockito.any(Destination.class));
			assertThatThrownBy(() -> messageHandler.handleMessage(message))
					.isInstanceOf(MessagingException.class)
					.hasCause(exception);
		}

		Mockito.verify(solaceMeterAccessor, Mockito.times(1))
				.recordMessage(Mockito.eq(producerProperties.getBindingName()), Mockito.any());
	}

	Message<String> getMessage(CorrelationData correlationData) {
		return MessageBuilder.withPayload("the payload")
				.setHeader(SolaceBinderHeaders.CONFIRM_CORRELATION, correlationData)
				.build();
	}

	private ErrorChannelSendingCorrelationKey getCorrelationKey() throws JCSMPException {
		Mockito.verify(messageProducer).send(xmlMessageCaptor.capture(), Mockito.any(Destination.class));
		return (ErrorChannelSendingCorrelationKey) xmlMessageCaptor.getValue().getCorrelationKey();
	}

	private ErrorChannelSendingCorrelationKey createCorrelationKey(CorrelationData correlationData, Message<?> msg) {
		ErrorChannelSendingCorrelationKey key = new ErrorChannelSendingCorrelationKey(
				msg,
				Mockito.mock(MessageChannel.class),
				new SolaceMessageHeaderErrorMessageStrategy());
		key.setConfirmCorrelation(correlationData);
		return key;
	}

	private ErrorChannelSendingCorrelationKey createCorrelationKey(CorrelationData correlationData) {
		Message<String> msg = MessageBuilder.withPayload("the empty payload")
				.build();
		return createCorrelationKey(correlationData, msg);
	}
}