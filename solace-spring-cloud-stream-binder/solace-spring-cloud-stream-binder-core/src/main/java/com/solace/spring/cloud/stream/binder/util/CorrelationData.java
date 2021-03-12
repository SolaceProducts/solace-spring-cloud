package com.solace.spring.cloud.stream.binder.util;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

public class CorrelationData {
	private final SettableListenableFuture<Void> future = new SettableListenableFuture<>();

	private Message<?> message;

	/**
	 * Return a future to check the success/failure of the publish operation.
	 * @return the future.
	 */
	public ListenableFuture<Void> getFuture() {
		return this.future;
	}

	public Message<?> getMessage() {
		return message;
	}

	public void setMessage(Message<?> message) {
		this.message = message;
	}

	void success() {
		this.future.set(null);
	}

	void failed(MessagingException cause) {
		this.future.setException(cause);
	}
}
