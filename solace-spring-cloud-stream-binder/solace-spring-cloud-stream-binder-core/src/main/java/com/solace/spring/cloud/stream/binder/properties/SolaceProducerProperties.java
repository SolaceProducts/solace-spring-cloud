package com.solace.spring.cloud.stream.binder.properties;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties.DEFAULTS_PREFIX;

@SuppressWarnings("ConfigurationProperties")
@ConfigurationProperties(DEFAULTS_PREFIX + ".producer")
public class SolaceProducerProperties extends SolaceCommonProperties {
	/**
	 * A mapping of required consumer groups to arrays of additional topic subscriptions to be applied on each consumer group’s queue.
	 * These subscriptions may also contain wildcards.
	 * The prefix property is not applied on these subscriptions.
	 */
	private Map<String,String[]> queueAdditionalSubscriptions = new HashMap<>();
	/**
	 * The list of headers to exclude from the published message. Excluding Solace message headers is not supported.
	 */
	private List<String> headerExclusions = new ArrayList<>();
	/**
	 * When set to true, irreversibly convert non-serializable headers to strings. An exception is thrown otherwise.
	 */
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
