package com.solace.spring.cloud.stream.binder.util;

import java.util.concurrent.CompletableFuture;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;

public class CorrelationData {
	private final CompletableFuture<Void> future = new CompletableFuture<>();

	private Message<?> message;

	/**
	 * Return a future to check the success/failure of the publish operation.
	 * @return the future.
	 */
	public CompletableFuture<Void> getFuture() {
		return this.future;
	}

	public Message<?> getMessage() {
		return message;
	}

	public void setMessage(Message<?> message) {
		this.message = message;
	}

	void success() {
		this.future.complete(null);
	}

	void failed(MessagingException cause) {
		this.future.completeExceptionally(cause);
	}
}
