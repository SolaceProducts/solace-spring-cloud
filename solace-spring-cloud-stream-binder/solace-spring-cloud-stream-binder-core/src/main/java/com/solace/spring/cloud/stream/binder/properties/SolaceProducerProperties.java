package com.solace.spring.cloud.stream.binder.properties;

import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;
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
	 * The type of destination messages are published to.
	 */
	private DestinationType destinationType = DestinationType.TOPIC;

	/**
	 * A SpEL expression for creating the consumer group’s queue name.
	 * Modifying this can cause naming conflicts between the queue names of consumer groups.
	 * While the default SpEL expression will consistently return a value adhering to <<Generated Queue Name Syntax>>,
	 * directly using the SpEL expression string is not supported. The default value for this config option is subject to change without notice.
	 */
	private String queueNameExpression = "'scst/' + (isAnonymous ? 'an/' : 'wk/') + (group?.trim() + '/') + 'plain/' + destination.trim().replaceAll('[*>]', '_')";

	/**
	 * A mapping of required consumer groups to queue name SpEL expressions.
	 * By default, queueNameExpression will be used to generate a required group’s queue name if it isn’t specified within this configuration option.
	 * Modifying this can cause naming conflicts between the queue names of consumer groups.
	 * While the default SpEL expression will consistently return a value adhering to <<Generated Queue Name Syntax>>,
	 * directly using the SpEL expression string is not supported. The default value for this config option is subject to change without notice.
	 */
	private Map<String, String> queueNameExpressionsForRequiredGroups = new HashMap<>();
	/**
	 * A mapping of required consumer groups to arrays of additional topic subscriptions to be applied on each consumer group’s queue.
	 * These subscriptions may also contain wildcards.
	 */
	private Map<String,String[]> queueAdditionalSubscriptions = new HashMap<>();
	/**
	 * The list of headers to exclude from the published message. Excluding Solace message headers is not supported.
	 */
	private List<String> headerExclusions = new ArrayList<>();

	/**
	 * The compatibility mode for message headers when they're being written to the SMF message.
	 *
	 * @deprecated since 6.0.0, scheduled for removal. Only headers which are natively supported by SMF are allowed to be written. Unsupported types will throw an exception.
	 */
	@Deprecated(forRemoval = true, since = "6.0.0")
	private SmfMessageHeaderWriteCompatibility headerTypeCompatibility = SmfMessageHeaderWriteCompatibility.NATIVE_ONLY;

	/**
	 * The compatibility mode for message payloads when they're being written to the SMF message.
	 *
	 * @deprecated since 6.0.0, scheduled for removal. Only payloads which are natively supported by SMF are allowed to be written. Unsupported types will throw an exception.
	 */
	@Deprecated(forRemoval = true, since = "6.0.0")
	private SmfMessagePayloadWriteCompatibility payloadTypeCompatibility = SmfMessagePayloadWriteCompatibility.NATIVE_ONLY;

	/**
	 * When set to true, irreversibly convert non-serializable headers to strings. An exception is thrown otherwise.
	 * Only applies when {@link #headerTypeCompatibility} is set to {@link SmfMessageHeaderWriteCompatibility#SERIALIZE_AND_ENCODE_NON_NATIVE_TYPES}.
	 *
	 * @deprecated since 6.0.0, scheduled for removal.
	 */
	@Deprecated(forRemoval = true, since = "6.0.0")
	private boolean nonserializableHeaderConvertToString = false;

	/**
	 * Mapping from Spring message header names to Solace message user property names.
	 */
	private Map<String, String> headerNameMapping = new HashMap<>();

	public DestinationType getDestinationType() {
		return destinationType;
	}

	public void setDestinationType(DestinationType destinationType) {
		this.destinationType = destinationType;
	}

	public String getQueueNameExpression() {
		return queueNameExpression;
	}

	public void setQueueNameExpression(String queueNameExpression) {
		this.queueNameExpression = queueNameExpression;
	}

	public Map<String, String> getQueueNameExpressionsForRequiredGroups() {
		return queueNameExpressionsForRequiredGroups;
	}

	public void setQueueNameExpressionsForRequiredGroups(Map<String, String> queueNameExpressionsForRequiredGroups) {
		this.queueNameExpressionsForRequiredGroups = queueNameExpressionsForRequiredGroups;
	}

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
	public boolean isNonserializableHeaderConvertToString() {
		return nonserializableHeaderConvertToString;
	}

	@Deprecated(forRemoval = true, since = "6.0.0")
	public void setNonserializableHeaderConvertToString(boolean nonserializableHeaderConvertToString) {
		this.nonserializableHeaderConvertToString = nonserializableHeaderConvertToString;
	}

	public Map<String, String> getHeaderNameMapping() {
		return headerNameMapping;
	}

	public void setHeaderNameMapping(Map<String, String> headerNameMapping) {
		this.headerNameMapping = headerNameMapping;
	}
}
