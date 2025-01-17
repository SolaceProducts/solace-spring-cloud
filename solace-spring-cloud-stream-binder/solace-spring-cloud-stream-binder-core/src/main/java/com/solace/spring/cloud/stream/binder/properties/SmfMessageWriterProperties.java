package com.solace.spring.cloud.stream.binder.properties;

import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;

import java.util.HashSet;
import java.util.Set;

public class SmfMessageWriterProperties {
	private Set<String> headerExclusions;
	private SmfMessageHeaderWriteCompatibility headerTypeCompatibility;
	private SmfMessagePayloadWriteCompatibility payloadTypeCompatibility;
	private boolean nonSerializableHeaderConvertToString;

	public SmfMessageWriterProperties(SolaceProducerProperties solaceProducerProperties) {
		this.headerExclusions = new HashSet<>(solaceProducerProperties.getHeaderExclusions());
		this.headerTypeCompatibility = solaceProducerProperties.getHeaderTypeCompatibility();
		this.payloadTypeCompatibility = solaceProducerProperties.getPayloadTypeCompatibility();
		this.nonSerializableHeaderConvertToString = solaceProducerProperties.isNonserializableHeaderConvertToString();
	}

	public Set<String> getHeaderExclusions() {
		return headerExclusions;
	}

	public void setHeaderExclusions(Set<String> headerExclusions) {
		this.headerExclusions = headerExclusions;
	}

	public SmfMessageHeaderWriteCompatibility getHeaderTypeCompatibility() {
		return headerTypeCompatibility;
	}

	public void setHeaderTypeCompatibility(SmfMessageHeaderWriteCompatibility headerTypeCompatibility) {
		this.headerTypeCompatibility = headerTypeCompatibility;
	}

	public SmfMessagePayloadWriteCompatibility getPayloadTypeCompatibility() {
		return payloadTypeCompatibility;
	}

	public void setPayloadTypeCompatibility(SmfMessagePayloadWriteCompatibility payloadTypeCompatibility) {
		this.payloadTypeCompatibility = payloadTypeCompatibility;
	}

	public boolean isNonSerializableHeaderConvertToString() {
		return nonSerializableHeaderConvertToString;
	}

	public void setNonSerializableHeaderConvertToString(boolean nonSerializableHeaderConvertToString) {
		this.nonSerializableHeaderConvertToString = nonSerializableHeaderConvertToString;
	}
}
