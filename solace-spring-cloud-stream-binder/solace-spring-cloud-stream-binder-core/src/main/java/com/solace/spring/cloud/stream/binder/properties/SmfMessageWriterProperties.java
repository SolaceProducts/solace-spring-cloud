package com.solace.spring.cloud.stream.binder.properties;

import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SmfMessageWriterProperties {
	private Set<String> headerExclusions;
	private SmfMessageHeaderWriteCompatibility headerTypeCompatibility;
	private SmfMessagePayloadWriteCompatibility payloadTypeCompatibility;
	private boolean nonSerializableHeaderConvertToString;
	private final Map<String, String> headerNameMapping;

	public SmfMessageWriterProperties(SolaceProducerProperties solaceProducerProperties) {
		this.headerExclusions = new HashSet<>(solaceProducerProperties.getHeaderExclusions());
		this.headerTypeCompatibility = solaceProducerProperties.getHeaderTypeCompatibility();
		this.payloadTypeCompatibility = solaceProducerProperties.getPayloadTypeCompatibility();
		this.nonSerializableHeaderConvertToString = solaceProducerProperties.isNonserializableHeaderConvertToString();
		this.headerNameMapping = new LinkedHashMap<>(Objects.requireNonNullElse(solaceProducerProperties.getHeaderNameMapping(), Map.of()));
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

	public Map<String, String> getHeaderNameMapping() {
		return headerNameMapping;
	}
}
