package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.ClosedFacilityException;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.StaleSessionException;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

/**
 * Unit tests for the DATAGO-134580 stale-flow recovery added to {@link ErrorQueueInfrastructure}.
 * The error-queue republish path borrows the session-default producer from
 * {@link JCSMPSessionProducerManager} and historically had no recovery logic when the broker
 * fanned out an unsolicited CloseFlow on that producer (reactive recreation in
 * {@code JCSMPOutboundMessageHandler} only protects per-binding producers, not the shared
 * session-default one).
 */
@ExtendWith(MockitoExtension.class)
class ErrorQueueInfrastructureTest {
	private static final String PRODUCER_KEY = "test-producer-key";
	private static final String ERROR_QUEUE_NAME = "test-error-queue";

	@Mock JCSMPSessionProducerManager producerManager;
	@Mock MessageContainer messageContainer;
	@Mock ErrorQueueRepublishCorrelationKey correlationKey;

	BytesXMLMessage inputMessage;
	SolaceConsumerProperties consumerProperties;
	ErrorQueueInfrastructure errorQueueInfrastructure;

	@BeforeEach
	void setup() {
		inputMessage = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
		consumerProperties = new SolaceConsumerProperties();
		errorQueueInfrastructure = new ErrorQueueInfrastructure(
				producerManager, PRODUCER_KEY, ERROR_QUEUE_NAME, consumerProperties);
		Mockito.when(messageContainer.getMessage()).thenReturn(inputMessage);
	}

	/**
	 * DATAGO-134580: proactive {@code producer.isClosed()} pre-check on the error-queue
	 * republish path. If the broker has already torn down the shared session-default
	 * producer before this {@code send(...)} runs, the very first error-queue publish
	 * should still succeed - the manager is asked to recreate the producer before send is
	 * attempted, and the fresh producer services the publish.
	 */
	@Test
	void testErrorQueueProducerRecreatedProactivelyOnIsClosed(
			@Mock XMLMessageProducer staleProducer,
			@Mock XMLMessageProducer freshProducer) throws Exception {
		Mockito.when(producerManager.get(PRODUCER_KEY)).thenReturn(staleProducer);
		Mockito.when(staleProducer.isClosed()).thenReturn(true);
		Mockito.when(producerManager.forceRecreate(staleProducer)).thenReturn(freshProducer);

		assertThatCode(() -> errorQueueInfrastructure.send(messageContainer, correlationKey))
				.as("Proactive recreate must allow the publish to succeed on the fresh producer")
				.doesNotThrowAnyException();

		// CAS contract: caller passes the observed (stale) reference so the manager only
		// recreates if it still holds that exact instance.
		Mockito.verify(producerManager).forceRecreate(staleProducer);
		Mockito.verify(freshProducer).send(any(XMLMessage.class), any(Destination.class));
		Mockito.verify(staleProducer, Mockito.never()).send(any(XMLMessage.class), any(Destination.class));
	}

	/**
	 * DATAGO-134580: reactive recreation when {@code send(...)} itself throws a
	 * stale-flow exception. The race window between our proactive {@code isClosed()}
	 * check and the actual send means the broker can tear the producer down mid-flight;
	 * in that case the exception must propagate so {@code ErrorQueueRepublishCorrelationKey}
	 * can retry, and the manager must be force-recreated so the next retry attempt picks up
	 * a fresh producer rather than re-using the dead one.
	 *
	 * <p>Parameterized over the three concrete JCSMP exception types we treat as
	 * stale-flow signals - the recovery contract must apply to all of them.
	 */
	@CartesianTest(name = "[{index}] exception={0}")
	void testErrorQueueProducerRecreatedReactivelyOnStaleSendException(
			@Values(strings = {"stale", "transport", "closed-facility"}) String exceptionType,
			@Mock XMLMessageProducer staleProducer) throws Exception {
		Mockito.when(producerManager.get(PRODUCER_KEY)).thenReturn(staleProducer);
		Mockito.when(staleProducer.isClosed()).thenReturn(false);

		JCSMPException sendError = switch (exceptionType) {
			case "stale" -> new StaleSessionException(
					"Tried to perform operation on a closed XML message producer",
					new JCSMPException("Received unsolicited CloseFlow for producer (503:Service Unavailable)."));
			case "transport" -> new JCSMPTransportException(
					"Received unsolicited CloseFlow for producer (503:Service Unavailable).");
			case "closed-facility" -> new ClosedFacilityException("Producer is closed");
			default -> throw new IllegalArgumentException("unknown exception type: " + exceptionType);
		};
		Mockito.doThrow(sendError).when(staleProducer).send(any(XMLMessage.class), any(Destination.class));

		assertThatThrownBy(() -> errorQueueInfrastructure.send(messageContainer, correlationKey))
				.as("Stale-flow send failure must propagate so the retry caller can re-attempt")
				.isInstanceOf(sendError.getClass());

		// The manager must have been asked to forceRecreate (with the observed stale
		// producer for CAS semantics) so the next retry by ErrorQueueRepublishCorrelationKey
		// gets a fresh producer instead of the dead one.
		Mockito.verify(producerManager).forceRecreate(staleProducer);
	}

	/**
	 * DATAGO-134580: a non-stale {@code JCSMPException} from {@code send(...)} must
	 * propagate normally and must <em>not</em> trigger a producer recreate. Guards
	 * against an over-broad reactive arm that would churn the shared producer on
	 * every transient publish error (e.g. a malformed message).
	 */
	@Test
	void testErrorQueueProducerNotRecreatedOnUnrelatedJCSMPException(
			@Mock XMLMessageProducer producer) throws Exception {
		Mockito.when(producerManager.get(PRODUCER_KEY)).thenReturn(producer);
		Mockito.when(producer.isClosed()).thenReturn(false);

		JCSMPException unrelated = new JCSMPException("Some unrelated publishing error");
		Mockito.doThrow(unrelated).when(producer).send(any(XMLMessage.class), any(Destination.class));

		assertThatThrownBy(() -> errorQueueInfrastructure.send(messageContainer, correlationKey))
				.isInstanceOf(JCSMPException.class)
				.hasMessage("Some unrelated publishing error");

		Mockito.verify(producerManager, Mockito.never()).forceRecreate(any());
	}

	/**
	 * DATAGO-134580: CAS contract verification. When two callers both observe the
	 * same stale producer and both call {@code forceRecreate(stale)}, the manager
	 * recreates exactly once - the second call returns the already-recreated
	 * resource without closing it. {@code ErrorQueueInfrastructure.send(...)} must
	 * use the value returned by {@code forceRecreate} (rather than its own observed
	 * reference) so it ends up using whatever the manager currently holds, not a
	 * resource that another caller has since closed and replaced.
	 */
	@Test
	void testErrorQueueProducerUsesManagerReturnedReferenceAfterForceRecreate(
			@Mock XMLMessageProducer staleProducer,
			@Mock XMLMessageProducer alreadyRecreatedByAnotherCaller) throws Exception {
		Mockito.when(producerManager.get(PRODUCER_KEY)).thenReturn(staleProducer);
		Mockito.when(staleProducer.isClosed()).thenReturn(true);
		// Simulate the CAS no-op outcome: another caller already replaced the stale
		// producer, so the manager's CAS does not recreate again - it returns the
		// already-installed replacement instead.
		Mockito.when(producerManager.forceRecreate(staleProducer))
				.thenReturn(alreadyRecreatedByAnotherCaller);

		assertThatCode(() -> errorQueueInfrastructure.send(messageContainer, correlationKey))
				.as("send must use the manager-returned reference (the already-installed replacement) " +
						"and not the locally-observed stale reference")
				.doesNotThrowAnyException();

		Mockito.verify(alreadyRecreatedByAnotherCaller).send(any(XMLMessage.class), any(Destination.class));
		Mockito.verify(staleProducer, Mockito.never()).send(any(XMLMessage.class), any(Destination.class));
	}
}