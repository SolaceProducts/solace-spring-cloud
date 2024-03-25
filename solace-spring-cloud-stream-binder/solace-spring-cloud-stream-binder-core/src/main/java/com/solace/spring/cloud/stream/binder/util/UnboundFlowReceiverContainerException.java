package com.solace.spring.cloud.stream.binder.util;

/**
 * <p>Flow receiver container is not bound to a flow receiver.</p>
 * <p>Typically caused by one of:</p>
 * <ul>
 * <li>{@link FlowReceiverContainer#unbind()}</li>
 * </ul>
 */
public class UnboundFlowReceiverContainerException extends Exception {
	public UnboundFlowReceiverContainerException(String message) {
		super(message);
	}
}
