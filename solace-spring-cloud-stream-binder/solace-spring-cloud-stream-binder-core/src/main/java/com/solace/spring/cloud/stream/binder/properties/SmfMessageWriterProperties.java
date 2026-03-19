package com.solace.spring.cloud.stream.binder.properties;

import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class SmfMessageWriterProperties {
	private Set<String> headerExclusions;

	@Deprecated(forRemoval = true, since = "6.0.0")
	private SmfMessageHeaderWriteCompatibility headerTypeCompatibility;
	@Deprecated(forRemoval = true, since = "6.0.0")
	private SmfMessagePayloadWriteCompatibility payloadTypeCompatibility;
	@Deprecated(forRemoval = true, since = "6.0.0")
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

	@Deprecated(forRemoval = true, since = "6.0.0")
	public SmfMessageHeaderWriteCompatibility getHeaderTypeCompatibility() {
		return headerTypeCompatibility;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public void setHeaderTypeCompatibility(SmfMessageHeaderWriteCompatibility headerTypeCompatibility) {
		this.headerTypeCompatibility = headerTypeCompatibility;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public SmfMessagePayloadWriteCompatibility getPayloadTypeCompatibility() {
		return payloadTypeCompatibility;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public void setPayloadTypeCompatibility(SmfMessagePayloadWriteCompatibility payloadTypeCompatibility) {
		this.payloadTypeCompatibility = payloadTypeCompatibility;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public boolean isNonSerializableHeaderConvertToString() {
		return nonSerializableHeaderConvertToString;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public void setNonSerializableHeaderConvertToString(boolean nonSerializableHeaderConvertToString) {
		this.nonSerializableHeaderConvertToString = nonSerializableHeaderConvertToString;
	}

	public Map<String, String> getHeaderNameMapping() {
		return headerNameMapping;
	}
}
