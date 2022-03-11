package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
	private long timeSentLastBatch = System.currentTimeMillis();
	private UUID currentFlowReceiverReferenceId;

	private static final Log logger = LogFactory.getLog(BatchCollector.class);

	public BatchCollector(SolaceConsumerProperties consumerProperties) {
		this.consumerProperties = consumerProperties;
		this.batchedMessages = new ArrayList<>(consumerProperties.getBatchMaxSize());
	}

	/**
	 * Add message to batch
	 * @param messageContainer message container
	 */
	public void addToBatch(MessageContainer messageContainer) {
		if (messageContainer == null) {
			return;
		}

		batchedMessages.add(messageContainer);
		UUID flowReceiverReferenceId = messageContainer.getFlowReceiverReferenceId();
		if (currentFlowReceiverReferenceId != null && !currentFlowReceiverReferenceId.equals(flowReceiverReferenceId)) {
			if (logger.isTraceEnabled()) {
				logger.trace(String.format("Added a message to batch, but its flow receiver reference ID was %s, " +
								"expected %s. Pruning stale messages from batch...",
						flowReceiverReferenceId, currentFlowReceiverReferenceId));
			}
			pruneStaleMessages();
		}
		currentFlowReceiverReferenceId = flowReceiverReferenceId;
	}

	/**
	 * Checks if batch is eligible to be collected.
	 * @return true if available (batch may be empty).
	 */
	public boolean isBatchAvailable() {
		return isBatchAvailableInternal() && (!pruneStaleMessages() || isBatchAvailableInternal());
	}

	public boolean isBatchAvailableInternal() {
		if (batchedMessages.size() < consumerProperties.getBatchMaxSize()) {
			long batchTimeDiff = System.currentTimeMillis() - timeSentLastBatch;
			if (consumerProperties.getBatchTimeout() == 0 || batchTimeDiff < consumerProperties.getBatchTimeout()) {
				if (logger.isTraceEnabled()) {
					logger.trace(String.format("Collecting batch... Size: %s, Time since last batch: %s ms",
							batchedMessages.size(), batchTimeDiff));
				}
				return false;
			} else if (logger.isTraceEnabled()) {
				logger.trace(String.format(
						"Batch timeout reached <time since last batch: %s ms>, processing batch of %s messages...",
						batchTimeDiff, batchedMessages.size()));
			}
		} else if (logger.isTraceEnabled()) {
			logger.trace(String.format("Max batch size reached, processing batch of %s messages...",
					batchedMessages.size()));
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
	 * Reset the timestamp of the last batch sent if the message batch is empty.
	 */
	public void resetLastSentTimeIfEmpty() {
		if (batchedMessages.isEmpty()) {
			resetLastSentTime();
		}
	}

	/**
	 * Callback to invoke when batch of messages have been processed.
	 */
	public void confirmDelivery() {
		resetLastSentTime();
		batchedMessages.clear();
	}

	/**
	 * Prune the batch of all stale messages
	 * @return true if messages were pruned
	 */
	private boolean pruneStaleMessages() {
		int prePrunedBatchSize = batchedMessages.size();
		boolean pruned = batchedMessages.removeIf(MessageContainer::isStale);
		if (logger.isTraceEnabled()) {
			logger.trace(String.format("Finished pruning stale messages from undelivered batch. Size: %s -> %s",
					prePrunedBatchSize, batchedMessages.size()));
		}
		return pruned;
	}

	private void resetLastSentTime() {
		timeSentLastBatch = System.currentTimeMillis();
		if (logger.isTraceEnabled()) {
			logger.trace("Timestamp of last batch sent was reset");
		}
	}
}
