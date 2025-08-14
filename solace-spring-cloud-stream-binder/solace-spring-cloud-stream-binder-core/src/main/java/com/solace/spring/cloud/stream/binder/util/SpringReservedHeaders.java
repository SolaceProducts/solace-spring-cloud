package com.solace.spring.cloud.stream.binder.util;

import org.springframework.integration.IntegrationMessageHeaderAccessor;
import org.springframework.messaging.MessageHeaders;
/**
 * Spring reserved headers
 */
public enum SpringReservedHeaders {
	CONTENT_TYPE(MessageHeaders.CONTENT_TYPE),
	ERROR_CHANNEL(MessageHeaders.ERROR_CHANNEL),
	ID(MessageHeaders.ID),
	TIMESTAMP(MessageHeaders.TIMESTAMP),
	//
	CORRELATION_ID(IntegrationMessageHeaderAccessor.CORRELATION_ID),
	SEQUENCE_NUMBER(IntegrationMessageHeaderAccessor.SEQUENCE_NUMBER),
	SEQUENCE_SIZE(IntegrationMessageHeaderAccessor.SEQUENCE_SIZE),
	EXPIRATION_DATE(IntegrationMessageHeaderAccessor.EXPIRATION_DATE),
	PRIORITY(IntegrationMessageHeaderAccessor.PRIORITY),
	DUPLICATE_MESSAGE(IntegrationMessageHeaderAccessor.DUPLICATE_MESSAGE),
	CLOSEABLE_RESOURCE(IntegrationMessageHeaderAccessor.CLOSEABLE_RESOURCE),
	DELIVERY_ATTEMPT(IntegrationMessageHeaderAccessor.DELIVERY_ATTEMPT),
	ACKNOWLEDGMENT_CALLBACK(IntegrationMessageHeaderAccessor.ACKNOWLEDGMENT_CALLBACK),	
	//
	SEQUENCE_DETAILS(IntegrationMessageHeaderAccessor.SEQUENCE_DETAILS),
	ROUTING_SLIP(IntegrationMessageHeaderAccessor.ROUTING_SLIP),
	;
	
	public static final String SOLACE_BACKUP_PREFIX = "solace_sbk_";
	
	private String springPredefinedHeader;
	private String solaceBackupHeader;
	
	private SpringReservedHeaders(String predefinedHeader) {
		this.springPredefinedHeader = predefinedHeader;
		this.solaceBackupHeader = String.format("%s%s", SOLACE_BACKUP_PREFIX, predefinedHeader);
	}
	
	public String getSpringPredefinedHeader() {
		return springPredefinedHeader;
	}
	
	public String getSolaceBackupHeader() {
		return solaceBackupHeader;
	}
}
