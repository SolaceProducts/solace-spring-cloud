package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.concurrent.TimeUnit;

import static com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties.DEFAULTS_PREFIX;

@SuppressWarnings("ConfigurationProperties")
@ConfigurationProperties(DEFAULTS_PREFIX + ".consumer")
public class SolaceConsumerProperties extends SolaceCommonProperties {
	/**
	 * Maximum wait time for polled consumers to receive a message from their consumer group queue.
	 */
	private int polledConsumerWaitTimeInMillis = 100;
	/**
	 * The maximum time to wait for all unacknowledged messages to be acknowledged before a flow receiver rebind.
	 * Will wait forever if set to a value less than 0.
	 */
	private long flowPreRebindWaitTimeout = TimeUnit.SECONDS.toMillis(10);

	/**
	 * An array of additional topic subscriptions to be applied on the consumer group queue.
	 * These subscriptions may also contain wildcards.
	 * The prefix property is not applied on these subscriptions.
	 */
	private String[] queueAdditionalSubscriptions = new String[0];
	/**
	 * Whether to include the group name in the queue name for non-anonymous consumer groups.
	 */
	@Deprecated
	private boolean useGroupNameInQueueName = true;

	/**
	 * A SpEL expression for creating the consumer group’s queue name.
	 * Modifying this can cause naming conflicts between the queue names of consumer groups.
	 * While the default SpEL expression will consistently return a value adhering to <<Generated Queue Name Syntax>>,
	 * directly using the SpEL expression string is not supported. The default value for this config option is subject to change without notice.
	 */
	private String queueNameExpression = "(properties.solace.queueNamePrefix?.trim()?.length() > 0 ? properties.solace.queueNamePrefix.trim() + '/' : '') + (properties.solace.useFamiliarityInQueueName ? (isAnonymous ? 'an' : 'wk') + '/' : '') + (isAnonymous ? group?.trim() + '/' : (properties.solace.useGroupNameInQueueName ? group?.trim() + '/' : '')) + (properties.solace.useDestinationEncodingInQueueName ? 'plain' + '/' : '') + destination.trim().replaceAll('[*>]', '_')";

	// Error Queue Properties ---------
	/**
	 * Whether to automatically create a durable error queue to which messages will be republished when message processing failures are encountered.
	 * Only applies once all internal retries have been exhausted.
	 */
	private boolean autoBindErrorQueue = false;
	/**
	 * Whether to provision durable queues for error queues when autoBindErrorQueue is true.
	 * This should only be set to false if you have externally pre-provisioned the required queue on the message broker.
	 */
	private boolean provisionErrorQueue = true;
	/**
	 * A SpEL expression for creating the error queue’s name.
	 * Modifying this can cause naming conflicts between the error queue names.
	 * While the default SpEL expression will consistently return a value adhering to <<Generated Queue Name Syntax>>,
	 * directly using the SpEL expression string is not supported. The default value for this config option is subject to change without notice.
	 */
	private String errorQueueNameExpression = "(properties.solace.queueNamePrefix?.trim()?.length() > 0 ? properties.solace.queueNamePrefix.trim() + '/' : '') + 'error/' + (properties.solace.useFamiliarityInQueueName ? (isAnonymous ? 'an' : 'wk') + '/' : '') + (isAnonymous ? group?.trim() + '/' : (properties.solace.useGroupNameInErrorQueueName ? group?.trim() + '/' : '')) + (properties.solace.useDestinationEncodingInQueueName ? 'plain' + '/' : '') + destination.trim().replaceAll('[*>]', '_')";
	/**
	 * A custom error queue name.
	 */
	@Deprecated
	private String errorQueueNameOverride = null;
	/**
	 * Whether to include the group name in the error queue name for non-anonymous consumer groups.
	 */
	@Deprecated
	private boolean useGroupNameInErrorQueueName = true;
	/**
	 * Maximum number of attempts to send a failed message to the error queue.
	 * When all delivery attempts have been exhausted, the failed message will be requeued.
	 */
	private long errorQueueMaxDeliveryAttempts = 3;
	/**
	 * Access type for the error queue.
	 */
	private int errorQueueAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	/**
	 * Permissions for the error queue.
	 */
	private int errorQueuePermission = EndpointProperties.PERMISSION_CONSUME;
	/**
	 * If specified, whether to notify sender if a message fails to be enqueued to the error queue.
	 */
	private Integer errorQueueDiscardBehaviour = null;
	/**
	 * Sets the maximum message redelivery count on the error queue. (Zero means retry forever).
	 */
	private Integer errorQueueMaxMsgRedelivery = null;
	/**
	 * Maximum message size for the error queue.
	 */
	private Integer errorQueueMaxMsgSize = null;
	/**
	 * Message spool quota for the error queue.
	 */
	private Integer errorQueueQuota = null;
	/**
	 * Whether the error queue respects Message TTL.
	 */
	private Boolean errorQueueRespectsMsgTtl = null;
	/**
	 * The eligibility for republished messages to be moved to a Dead Message Queue.
	 */
	private Boolean errorMsgDmqEligible = null;
	/**
	 * The number of milliseconds before republished messages are discarded or moved to a Dead Message Queue.
	 */
	private Long errorMsgTtl = null;
	// ------------------------

	public int getPolledConsumerWaitTimeInMillis() {
		return polledConsumerWaitTimeInMillis;
	}

	public void setPolledConsumerWaitTimeInMillis(int polledConsumerWaitTimeInMillis) {
		this.polledConsumerWaitTimeInMillis = polledConsumerWaitTimeInMillis;
	}

	public long getFlowPreRebindWaitTimeout() {
		return flowPreRebindWaitTimeout;
	}

	public void setFlowPreRebindWaitTimeout(long flowPreRebindWaitTimeout) {
		this.flowPreRebindWaitTimeout = flowPreRebindWaitTimeout;
	}

	public String[] getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(String[] queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
	}

	public boolean isUseGroupNameInQueueName() {
		return useGroupNameInQueueName;
	}

	public void setUseGroupNameInQueueName(boolean useGroupNameInQueueName) {
		this.useGroupNameInQueueName = useGroupNameInQueueName;
	}

	public String getQueueNameExpression() {
		return queueNameExpression;
	}

	public void setQueueNameExpression(String queueNameExpression) {
		this.queueNameExpression = queueNameExpression;
	}

	public boolean isAutoBindErrorQueue() {
		return autoBindErrorQueue;
	}

	public void setAutoBindErrorQueue(boolean autoBindErrorQueue) {
		this.autoBindErrorQueue = autoBindErrorQueue;
	}

	public boolean isProvisionErrorQueue() {
		return provisionErrorQueue;
	}

	public void setProvisionErrorQueue(boolean provisionErrorQueue) {
		this.provisionErrorQueue = provisionErrorQueue;
	}

	public String getErrorQueueNameExpression() {
		return errorQueueNameExpression;
	}

	public void setErrorQueueNameExpression(String errorQueueNameExpression) {
		this.errorQueueNameExpression = errorQueueNameExpression;
	}

	public String getErrorQueueNameOverride() {
		return errorQueueNameOverride;
	}

	public void setErrorQueueNameOverride(String errorQueueNameOverride) {
		this.errorQueueNameOverride = errorQueueNameOverride;
	}

	public boolean isUseGroupNameInErrorQueueName() {
		return useGroupNameInErrorQueueName;
	}

	public void setUseGroupNameInErrorQueueName(boolean useGroupNameInErrorQueueName) {
		this.useGroupNameInErrorQueueName = useGroupNameInErrorQueueName;
	}

	public long getErrorQueueMaxDeliveryAttempts() {
		return errorQueueMaxDeliveryAttempts;
	}

	public void setErrorQueueMaxDeliveryAttempts(long errorQueueMaxDeliveryAttempts) {
		this.errorQueueMaxDeliveryAttempts = errorQueueMaxDeliveryAttempts;
	}

	public int getErrorQueueAccessType() {
		return errorQueueAccessType;
	}

	public void setErrorQueueAccessType(int errorQueueAccessType) {
		this.errorQueueAccessType = errorQueueAccessType;
	}

	public int getErrorQueuePermission() {
		return errorQueuePermission;
	}

	public void setErrorQueuePermission(int errorQueuePermission) {
		this.errorQueuePermission = errorQueuePermission;
	}

	public Integer getErrorQueueDiscardBehaviour() {
		return errorQueueDiscardBehaviour;
	}

	public void setErrorQueueDiscardBehaviour(Integer errorQueueDiscardBehaviour) {
		this.errorQueueDiscardBehaviour = errorQueueDiscardBehaviour;
	}

	public Integer getErrorQueueMaxMsgRedelivery() {
		return errorQueueMaxMsgRedelivery;
	}

	public void setErrorQueueMaxMsgRedelivery(Integer errorQueueMaxMsgRedelivery) {
		this.errorQueueMaxMsgRedelivery = errorQueueMaxMsgRedelivery;
	}

	public Integer getErrorQueueMaxMsgSize() {
		return errorQueueMaxMsgSize;
	}

	public void setErrorQueueMaxMsgSize(Integer errorQueueMaxMsgSize) {
		this.errorQueueMaxMsgSize = errorQueueMaxMsgSize;
	}

	public Integer getErrorQueueQuota() {
		return errorQueueQuota;
	}

	public void setErrorQueueQuota(Integer errorQueueQuota) {
		this.errorQueueQuota = errorQueueQuota;
	}

	public Boolean getErrorQueueRespectsMsgTtl() {
		return errorQueueRespectsMsgTtl;
	}

	public void setErrorQueueRespectsMsgTtl(Boolean errorQueueRespectsMsgTtl) {
		this.errorQueueRespectsMsgTtl = errorQueueRespectsMsgTtl;
	}

	public Boolean getErrorMsgDmqEligible() {
		return errorMsgDmqEligible;
	}

	public void setErrorMsgDmqEligible(Boolean errorMsgDmqEligible) {
		this.errorMsgDmqEligible = errorMsgDmqEligible;
	}

	public Long getErrorMsgTtl() {
		return errorMsgTtl;
	}

	public void setErrorMsgTtl(Long errorMsgTtl) {
		this.errorMsgTtl = errorMsgTtl;
	}
}
