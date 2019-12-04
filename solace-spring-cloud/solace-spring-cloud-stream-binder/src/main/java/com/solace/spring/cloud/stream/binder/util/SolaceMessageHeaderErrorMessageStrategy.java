package com.solace.spring.cloud.stream.binder.util;

import org.springframework.core.AttributeAccessor;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SolaceMessageHeaderErrorMessageStrategy implements ErrorMessageStrategy {
	public static final String INPUT_MESSAGE = "inputMessage";
	public static final String SOLACE_RAW_MESSAGE = "solace_raw_message";

	@Override
	public ErrorMessage buildErrorMessage(Throwable throwable, AttributeAccessor attributeAccessor) {
		Object inputMessage = attributeAccessor == null ? null : attributeAccessor.getAttribute(INPUT_MESSAGE);
		Map<String, Object> headers = attributeAccessor == null ? new HashMap() : Collections.singletonMap(SOLACE_RAW_MESSAGE, attributeAccessor.getAttribute(SOLACE_RAW_MESSAGE));
		return new ErrorMessage(throwable, headers, inputMessage instanceof Message ? (Message)inputMessage : null);
	}
}
