package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Collector which batches message.
 * Message batches can be retrieved from this collector only when batching requirements have been met.
 */
public class BatchCollector {
	private final SolaceConsumerProperties consumerProperties;
	private final List<MessageContainer> batchedMessages;
	private long batchCollectionStartTimestamp = System.currentTimeMillis();
	private boolean isLastMessageNull = false;
	private UUID currentFlowReceiverReferenceId;

	private static final Logger LOGGER = LoggerFactory.getLogger(BatchCollector.class);

	public BatchCollector(SolaceConsumerProperties consumerProperties) {
		this.consumerProperties = consumerProperties;
		this.batchedMessages = new ArrayList<>(consumerProperties.getBatchMaxSize());
	}

	/**
	 * Add message to batch
	 * @param messageContainer message container
	 */
	public void addToBatch(@Nullable MessageContainer messageContainer) {
		if (messageContainer == null) {
			isLastMessageNull = true;
			return;
		}

		isLastMessageNull = false;
		batchedMessages.add(messageContainer);
		UUID flowReceiverReferenceId = messageContainer.getFlowReceiverReferenceId();
		if (currentFlowReceiverReferenceId != null && !currentFlowReceiverReferenceId.equals(flowReceiverReferenceId)) {
			LOGGER.trace("Added a message to batch, but its flow receiver reference ID was {}, expected {}. " +
							"Pruning stale messages from batch...",
					flowReceiverReferenceId, currentFlowReceiverReferenceId);
			pruneStaleMessages();
		}
		currentFlowReceiverReferenceId = flowReceiverReferenceId;
	}

	/**
	 * Checks if batch is eligible to be collected.
	 * @return true if available (batch may be empty).
	 */
	public boolean isBatchAvailable() {
		// must be able to return an empty batch so that polled consumers don't block indefinitely
		return isBatchAvailableInternal() && (!pruneStaleMessages() || isBatchAvailableInternal());
	}

	boolean isBatchAvailableInternal() {
		if (batchedMessages.size() < consumerProperties.getBatchMaxSize()) {
			switch (consumerProperties.getBatchWaitStrategy()) {
				case RESPECT_TIMEOUT -> {
					long batchTimeDiff = System.currentTimeMillis() - batchCollectionStartTimestamp;
					if (consumerProperties.getBatchTimeout() == 0 || batchTimeDiff < consumerProperties.getBatchTimeout()) {
						if (!isLastMessageNull) {
							LOGGER.trace("Collecting batch... Size: {}, Time elapsed: {} ms",
									batchedMessages.size(), batchTimeDiff);
						}
						return false;
					} else {
						LOGGER.trace(
								"Batch timeout reached <time elapsed: {} ms>, processing batch of {} messages...",
								batchTimeDiff, batchedMessages.size());
					}
				}
				case IMMEDIATE -> {
					if (isLastMessageNull) {
						LOGGER.trace("Last message was null, processing batch of {} messages...", batchedMessages.size());
					} else {
						return false;
					}
				}
				default -> throw new UnsupportedOperationException("Unsupported batch wait strategy: " +
						consumerProperties.getBatchWaitStrategy());
			}
		} else {
			LOGGER.trace("Max batch size reached, processing batch of {} messages...", batchedMessages.size());
		}
		return true;
	}

	/**
	 * Retrieve the batch. After processing, {@link #confirmDelivery()} must be invoked to clear the batch
	 * from the collector.
	 * @return a non-empty batch of messages if available.
	 */
	public Optional<List<MessageContainer>> collectBatchIfAvailable() {
		return isBatchAvailable() ? Optional.of(batchedMessages)
				.filter(b -> !b.isEmpty())
				.map(Collections::unmodifiableList) :
				Optional.empty();
	}

	/**
	 * Reset batch collection start timestamp if the message batch is empty.
	 */
	public void resetBatchCollectionStartTimestampIfEmpty() {
		if (batchedMessages.isEmpty()) {
			resetBatchCollectionStartTimestamp();
		}
	}

	/**
	 * Callback to invoke when batch of messages have been processed.
	 */
	public void confirmDelivery() {
		LOGGER.trace("Confirmed delivery of batch of {} messages", batchedMessages.size());
		resetBatchCollectionStartTimestamp();
		isLastMessageNull = false;
		batchedMessages.clear();
	}

	/**
	 * Prune the batch of all stale messages
	 * @return true if messages were pruned
	 */
	private boolean pruneStaleMessages() {
		int prePrunedBatchSize = batchedMessages.size();
		boolean pruned = batchedMessages.removeIf(MessageContainer::isStale);
		LOGGER.trace("Finished pruning stale messages from undelivered batch. Size: {} -> {}", prePrunedBatchSize,
				batchedMessages.size());
		return pruned;
	}

	private void resetBatchCollectionStartTimestamp() {
		batchCollectionStartTimestamp = System.currentTimeMillis();
		LOGGER.trace("Batch collection start time was reset");
	}
}
