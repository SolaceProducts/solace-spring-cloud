package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.support.ErrorMessageStrategy;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SolaceMessageHeaderErrorMessageStrategy implements ErrorMessageStrategy {
	public static final String INPUT_MESSAGE = "inputMessage";
	public static final String SOLACE_RAW_MESSAGE = SolaceBinderHeaders.RAW_MESSAGE;

	@Override
	public ErrorMessage buildErrorMessage(Throwable throwable, AttributeAccessor attributeAccessor) {
		Object inputMessage;
		Map<String, Object> headers;
		if (attributeAccessor == null) {
			inputMessage = null;
			headers = new HashMap<>();
		} else {
			inputMessage = attributeAccessor.getAttribute(INPUT_MESSAGE);
			headers = Collections.singletonMap(SolaceBinderHeaders.RAW_MESSAGE,
					attributeAccessor.getAttribute(SOLACE_RAW_MESSAGE));
		}
		return new ErrorMessage(throwable, headers, inputMessage instanceof Message ? (Message<?>) inputMessage : null);
	}
}
