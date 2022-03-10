package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.RETRY_INTERVAL;
import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class BatchCollectorTest {

	@Test
	public void testAddAndReceive() {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		List<MessageContainer> messageContainers = IntStream.range(0, consumerProperties.getBatchMaxSize())
				.mapToObj(i -> Mockito.mock(MessageContainer.class))
				.collect(Collectors.toList());

		BatchCollector batchCollector = new BatchCollector(consumerProperties);

		assertThat(messageContainers).allSatisfy(messageContainer -> {
			// check first since after the last added message, the asserts would fail
			assertFalse(batchCollector.isBatchAvailable(), "batch is unexpectedly available");
			assertThat(batchCollector.collectBatchIfAvailable()).isEmpty();
			batchCollector.addToBatch(messageContainer);
		});

		assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
		assertThat(batchCollector.collectBatchIfAvailable()).get().asList()
				.containsExactlyElementsOf(messageContainers);

		// Do it again since we haven't confirmed delivery
		assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
		assertThat(batchCollector.collectBatchIfAvailable()).get().asList()
				.containsExactlyElementsOf(messageContainers);

		batchCollector.confirmDelivery();
		assertFalse(batchCollector.isBatchAvailable(), "batch is unexpectedly available");
		assertThat(batchCollector.collectBatchIfAvailable()).isEmpty();
	}

	@Test
	public void testReachedBatchTimeout(@Mock MessageContainer messageContainer) throws InterruptedException {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setBatchTimeout((int) TimeUnit.SECONDS.toMillis(5));
		BatchCollector batchCollector = new BatchCollector(consumerProperties);
		batchCollector.addToBatch(messageContainer);
		retryAssert(() -> {
			assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
			assertThat(batchCollector.collectBatchIfAvailable()).get().asList().containsExactly(messageContainer);
		}, consumerProperties.getBatchTimeout() + RETRY_INTERVAL.multipliedBy(2).toMillis(),
				TimeUnit.MILLISECONDS);
	}

	@Test
	public void testPruneStaleMessagesWhenAdding(@Mock MessageContainer messageContainer1,
												@Mock MessageContainer messageContainer2,
												@Mock MessageContainer messageContainer3,
												@Mock MessageContainer messageContainer4) {
		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		consumerProperties.setBatchMaxSize(2);

		AtomicBoolean staleFlag = new AtomicBoolean(false);
		UUID initialFlowReceiverReferenceId = UUID.randomUUID();
		Mockito.when(messageContainer1.getFlowReceiverReferenceId()).thenReturn(initialFlowReceiverReferenceId);
		Mockito.when(messageContainer2.getFlowReceiverReferenceId()).thenReturn(initialFlowReceiverReferenceId);
		Mockito.when(messageContainer1.isStale()).thenAnswer((Answer<Boolean>) invocation -> staleFlag.get());
		Mockito.when(messageContainer2.isStale()).thenAnswer((Answer<Boolean>) invocation -> staleFlag.get());

		UUID newFlowReceiverReferenceId = UUID.randomUUID();
		Mockito.when(messageContainer3.getFlowReceiverReferenceId()).thenReturn(newFlowReceiverReferenceId);
		Mockito.when(messageContainer4.getFlowReceiverReferenceId()).thenReturn(newFlowReceiverReferenceId);

		BatchCollector batchCollector = new BatchCollector(consumerProperties);

		batchCollector.addToBatch(messageContainer1);
		batchCollector.addToBatch(messageContainer2);
		assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
		assertThat(batchCollector.collectBatchIfAvailable()).get().asList()
				.containsExactly(messageContainer1, messageContainer2);

		staleFlag.set(true);
		batchCollector.addToBatch(messageContainer3);
		assertFalse(batchCollector.isBatchAvailable(), "batch is unexpectedly available");
		assertThat(batchCollector.collectBatchIfAvailable()).isEmpty();

		batchCollector.addToBatch(messageContainer4);
		assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
		assertThat(batchCollector.collectBatchIfAvailable()).get().asList()
				.containsExactly(messageContainer3, messageContainer4);
	}

	@ParameterizedTest(name = "[{index}] testTimeout={0}")
	@ValueSource(booleans = {false, true})
	public void testPruneStaleMessagesAfterTimeout(boolean testTimeout,
												   @Mock MessageContainer messageContainer1,
												   @Mock MessageContainer messageContainer2) throws Exception {
		Mockito.when(messageContainer1.getFlowReceiverReferenceId()).thenReturn(UUID.randomUUID());
		Mockito.when(messageContainer1.isStale()).thenReturn(true);
		Mockito.when(messageContainer2.getFlowReceiverReferenceId()).thenReturn(UUID.randomUUID());

		SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
		if (testTimeout) {
			consumerProperties.setBatchTimeout(1000);
		} else {
			consumerProperties.setBatchMaxSize(1);
			consumerProperties.setBatchTimeout(0);
		}

		BatchCollector batchCollector = new BatchCollector(consumerProperties);

		batchCollector.addToBatch(messageContainer1);
		if (testTimeout) {
			assertFalse(batchCollector.isBatchAvailable(), "batch is unexpectedly available");
			Thread.sleep(consumerProperties.getBatchTimeout() + 1000);
		}
		assertThat(batchCollector.isBatchAvailable()).isEqualTo(testTimeout);
		assertThat(batchCollector.collectBatchIfAvailable()).isEmpty();

		batchCollector.addToBatch(messageContainer2);
		assertTrue(batchCollector.isBatchAvailable(), "batch is not available");
		assertThat(batchCollector.collectBatchIfAvailable()).get().asList().containsExactly(messageContainer2);
	}
}
