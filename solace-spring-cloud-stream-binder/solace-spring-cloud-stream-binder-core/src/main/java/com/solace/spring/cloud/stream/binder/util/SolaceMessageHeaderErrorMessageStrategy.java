package com.solace.spring.cloud.stream.binder.util;

import org.springframework.core.AttributeAccessor;
import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.integration.support.ErrorMessageUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;

import java.util.HashMap;
import java.util.Map;

public class SolaceMessageHeaderErrorMessageStrategy implements ErrorMessageStrategy {
	public static final String ATTR_SOLACE_RAW_MESSAGE = "solace_sourceData";
	public static final String ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK = "solace_acknowledgmentCallback";

	@Override
	public ErrorMessage buildErrorMessage(Throwable throwable, AttributeAccessor attributeAccessor) {
		Object inputMessage;
		Map<String, Object> headers = new HashMap<>();
		if (attributeAccessor == null) {
			inputMessage = null;
		} else {
			inputMessage = attributeAccessor.getAttribute(ErrorMessageUtils.INPUT_MESSAGE_CONTEXT_KEY);
			Object sourceData = attributeAccessor.getAttribute(ATTR_SOLACE_RAW_MESSAGE);
			if (sourceData != null) {
				headers.put(IntegrationMessageHeaderAccessor.SOURCE_DATA, sourceData);
			}
			Object ackCallback = attributeAccessor.getAttribute(ATTR_SOLACE_ACKNOWLEDGMENT_CALLBACK);
			if (ackCallback != null) {
				headers.put(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK, ackCallback);
			}
		}
		return inputMessage instanceof Message ? new ErrorMessage(throwable, headers, (Message<?>) inputMessage) :
				new ErrorMessage(throwable, headers);
	}
}
