package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.MessageContainer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class BatchCollector {
	private final SolaceConsumerProperties consumerProperties;
	private final List<MessageContainer> batchedMessages;
	private long timeSentLastBatch = System.currentTimeMillis();

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
		if (messageContainer != null) {
			batchedMessages.add(messageContainer);
		}
	}

	/**
	 * Checks if batch is eligible to be collected.
	 * @return true if available (batch may be empty).
	 */
	public boolean isBatchAvailable() {
		if (batchedMessages.size() < consumerProperties.getBatchMaxSize()) {
			long batchTimeDiff = System.currentTimeMillis() - timeSentLastBatch;
			if (consumerProperties.getBatchTimeout() == 0 || batchTimeDiff < consumerProperties.getBatchTimeout()) {
				if (logger.isTraceEnabled()) {
					logger.trace(String.format("Collecting batch... Size: %s, Time since last batch: %s ms",
							batchedMessages.size(), batchTimeDiff));
				}
				return false;
			} else if (logger.isTraceEnabled()) {
				logger.trace(String.format("Batch timeout reached, processing batch of %s messages...",
						batchedMessages.size()));
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

	private void resetLastSentTime() {
		timeSentLastBatch = System.currentTimeMillis();
		if (logger.isTraceEnabled()) {
			logger.trace("Timestamp of last batch sent was reset");
		}
	}
}
