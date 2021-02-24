package com.solace.spring.cloud.stream.binder.properties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolaceProducerProperties extends SolaceCommonProperties {
	private Map<String,String[]> queueAdditionalSubscriptions = new HashMap<>();
	private List<String> headerExclusions = new ArrayList<>();
	private boolean nonserializableHeaderConvertToString = false;

	public Map<String, String[]> getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(Map<String, String[]> queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
	}

	public List<String> getHeaderExclusions() {
		return headerExclusions;
	}

	public void setHeaderExclusions(List<String> headerExclusions) {
		this.headerExclusions = headerExclusions;
	}

	public boolean isNonserializableHeaderConvertToString() {
		return nonserializableHeaderConvertToString;
	}

	public void setNonserializableHeaderConvertToString(boolean nonserializableHeaderConvertToString) {
		this.nonserializableHeaderConvertToString = nonserializableHeaderConvertToString;
	}
}
