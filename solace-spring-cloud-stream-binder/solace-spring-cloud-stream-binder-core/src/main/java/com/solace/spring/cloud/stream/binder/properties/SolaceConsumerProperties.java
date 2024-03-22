package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;
import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

import jakarta.validation.constraints.Min;
import java.util.concurrent.TimeUnit;

import static com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties.DEFAULTS_PREFIX;

@SuppressWarnings("ConfigurationProperties")
@ConfigurationProperties(DEFAULTS_PREFIX + ".consumer")
public class SolaceConsumerProperties extends SolaceCommonProperties {
	/**
	 * <p>The maximum number of messages per batch.</p>
	 * <p>Only applicable when {@code batchMode} is {@code true}.</p>
	 */
	@Min(1)
	private int batchMaxSize = 255;

	/**
	 * <p>The maximum wait time in milliseconds to receive a batch of messages. If this timeout is reached, then the
	 * messages that have already been received will be used to create the batch. A value of {@code 0} means wait
	 * forever.</p>
	 * <p>Only applicable when {@code batchMode} is {@code true}.</p>
	 */
	@Min(0)
	private int batchTimeout = 5000;

	/**
	 * Maximum wait time for polled consumers to receive a message from their consumer group queue.
	 * <p>Only applicable when {@code batchMode} is {@code false}.</p>
	 */
	private int polledConsumerWaitTimeInMillis = 100;
	/**
	 * The maximum time to wait for all unacknowledged messages to be acknowledged before a flow receiver rebind.
	 * Will wait forever if set to a value less than 0.
	 */
	private long flowPreRebindWaitTimeout = TimeUnit.SECONDS.toMillis(10);

	/**
	 * The initial interval (milliseconds) to back-off when rebinding a flow.
	 */
	@Min(1)
	private long flowRebindBackOffInitialInterval = TimeUnit.SECONDS.toMillis(1);

	/**
	 * The maximum interval (milliseconds) to back-off when rebinding a flow.
	 */
	@Min(1)
	private long flowRebindBackOffMaxInterval = TimeUnit.SECONDS.toMillis(30);

	/**
	 * The multiplier to apply to the back-off interval between each rebind of a flow.
	 */
	@Min(1)
	private double flowRebindBackOffMultiplier = 1.5;

	/**
	 * An array of additional topic subscriptions to be applied on the consumer group queue.
	 * These subscriptions may also contain wildcards.
	 */
	private String[] queueAdditionalSubscriptions = new String[0];

	/**
	 * A SpEL expression for creating the consumer group’s queue name.
	 * Modifying this can cause naming conflicts between the queue names of consumer groups.
	 * While the default SpEL expression will consistently return a value adhering to <<Generated Queue Name Syntax>>,
	 * directly using the SpEL expression string is not supported. The default value for this config option is subject to change without notice.
	 */
	private String queueNameExpression = "'scst/' + (isAnonymous ? 'an/' : 'wk/') + (group?.trim() + '/') + 'plain/' + destination.trim().replaceAll('[*>]', '_')";

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
	private String errorQueueNameExpression = "'scst/error/' + (isAnonymous ? 'an/' : 'wk/') + (group?.trim() + '/') + 'plain/' + destination.trim().replaceAll('[*>]', '_')";

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

	/**
	 * The list of headers to exclude when converting consumed Solace message to Spring message.
	 */
	private List<String> headerExclusions = new ArrayList<>();

	public int getBatchMaxSize() {
		return batchMaxSize;
	}

	public void setBatchMaxSize(int batchMaxSize) {
		Assert.isTrue(batchMaxSize >= 1, "max batch size must be greater than 0");
		this.batchMaxSize = batchMaxSize;
	}

	public int getBatchTimeout() {
		return batchTimeout;
	}

	public void setBatchTimeout(int batchTimeout) {
		Assert.isTrue(batchTimeout >= 0, "batch timeout must be greater than or equal to 0");
		this.batchTimeout = batchTimeout;
	}

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

	public long getFlowRebindBackOffInitialInterval() {
		return flowRebindBackOffInitialInterval;
	}

	public void setFlowRebindBackOffInitialInterval(long flowRebindBackOffInitialInterval) {
		Assert.isTrue(flowRebindBackOffInitialInterval >= 1,
				"flow rebind back-off initial interval must be greater than or equal to 1");
		this.flowRebindBackOffInitialInterval = flowRebindBackOffInitialInterval;
	}

	public long getFlowRebindBackOffMaxInterval() {
		return flowRebindBackOffMaxInterval;
	}

	public void setFlowRebindBackOffMaxInterval(long flowRebindBackOffMaxInterval) {
		Assert.isTrue(flowRebindBackOffMaxInterval >= 1,
				"flow rebind back-off max interval must be greater than or equal to 1");
		this.flowRebindBackOffMaxInterval = flowRebindBackOffMaxInterval;
	}

	public double getFlowRebindBackOffMultiplier() {
		return flowRebindBackOffMultiplier;
	}

	public void setFlowRebindBackOffMultiplier(double flowRebindBackOffMultiplier) {
		Assert.isTrue(flowRebindBackOffMultiplier >= 1.0,
				"flow rebind back-off multiplier must be greater than or equal to 1.0");
		this.flowRebindBackOffMultiplier = flowRebindBackOffMultiplier;
	}

	public String[] getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(String[] queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
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

	public List<String> getHeaderExclusions() {
		return headerExclusions;
	}

	public void setHeaderExclusions(List<String> headerExclusions) {
		this.headerExclusions = headerExclusions;
	}
}
