package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;

public class SolaceCommonProperties {
	/**
	 * Whether to provision durable queues for non-anonymous consumer groups.
	 * This should only be set to false if you have externally pre-provisioned the required queue on the message broker.
	 */
	private boolean provisionDurableQueue = true;

	/**
	 * Whether to add topic subscriptions to durable queues for non-anonymous consumer groups.
	 * This should only be set to false if you have externally pre-added the required topic subscriptions (the destination topic should be added at minimum)
	 * on the consumer group’s queue on the message broker. This property also applies to topics added by the queueAdditionalSubscriptions property.
	 */
  @Deprecated
	private boolean provisionSubscriptionsToDurableQueue = true;
	/**
	 * Whether to add the Destination as a subscription to queue during provisioning.
	 */
	private boolean addDestinationAsSubscriptionToQueue = true;

	// Queue Properties -------
	/**
	 * Naming prefix for all queues.
	 */
	private String queueNamePrefix = "scst";
	/**
	 * When set to true, the familiarity modifier, wk/an, is included in the generated queue name.
	 */
	private boolean useFamiliarityInQueueName = true;
	/**
	 * When set to true, the destination encoding (plain), is included in the generated queue name.
	 */
	private boolean useDestinationEncodingInQueueName = true;

	/**
	 * Access type for the consumer group queue.
	 */
	private int queueAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	/**
	 * Permissions for the consumer group queue.
	 */
	private int queuePermission = EndpointProperties.PERMISSION_CONSUME;
	/**
	 * If specified, whether to notify sender if a message fails to be enqueued to the consumer group queue.
	 */
	private Integer queueDiscardBehaviour = null;
	/**
	 * Sets the maximum message redelivery count on consumer group queue. (Zero means retry forever).
	 */
	private Integer queueMaxMsgRedelivery = null;
	/**
	 * Maximum message size for the consumer group queue.
	 */
	private Integer queueMaxMsgSize = null;
	/**
	 * Message spool quota for the consumer group queue.
	 */
	private Integer queueQuota = null;
	/**
	 * Whether the consumer group queue respects Message TTL.
	 */
	private Boolean queueRespectsMsgTtl = null;
	// ------------------------

	public boolean isProvisionDurableQueue() {
		return provisionDurableQueue;
	}

	public void setProvisionDurableQueue(boolean provisionDurableQueue) {
		this.provisionDurableQueue = provisionDurableQueue;
	}

	public boolean isProvisionSubscriptionsToDurableQueue() {
		return provisionSubscriptionsToDurableQueue;
	}

	public void setProvisionSubscriptionsToDurableQueue(boolean provisionSubscriptionsToDurableQueue) {
		this.provisionSubscriptionsToDurableQueue = provisionSubscriptionsToDurableQueue;
	}

	public boolean isAddDestinationAsSubscriptionToQueue() {
		return addDestinationAsSubscriptionToQueue;
	}

	public void setAddDestinationAsSubscriptionToQueue(boolean addDestinationAsSubscriptionToQueue) {
		this.addDestinationAsSubscriptionToQueue = addDestinationAsSubscriptionToQueue;
	}

	public String getQueueNamePrefix() {
		return queueNamePrefix;
	}

	public void setQueueNamePrefix(String queueNamePrefix) {
		this.queueNamePrefix = queueNamePrefix;
	}

	public boolean isUseFamiliarityInQueueName() {
		return useFamiliarityInQueueName;
	}

	public void setUseFamiliarityInQueueName(boolean useFamiliarityInQueueName) {
		this.useFamiliarityInQueueName = useFamiliarityInQueueName;
	}

	public boolean isUseDestinationEncodingInQueueName() {
		return useDestinationEncodingInQueueName;
	}

	public void setUseDestinationEncodingInQueueName(boolean useDestinationEncodingInQueueName) {
		this.useDestinationEncodingInQueueName = useDestinationEncodingInQueueName;
	}

	public int getQueueAccessType() {
		return queueAccessType;
	}

	public void setQueueAccessType(int queueAccessType) {
		this.queueAccessType = queueAccessType;
	}

	public int getQueuePermission() {
		return queuePermission;
	}

	public void setQueuePermission(int queuePermission) {
		this.queuePermission = queuePermission;
	}

	public Integer getQueueDiscardBehaviour() {
		return queueDiscardBehaviour;
	}

	public void setQueueDiscardBehaviour(Integer queueDiscardBehaviour) {
		this.queueDiscardBehaviour = queueDiscardBehaviour;
	}

	public Integer getQueueMaxMsgRedelivery() {
		return queueMaxMsgRedelivery;
	}

	public void setQueueMaxMsgRedelivery(Integer queueMaxMsgRedelivery) {
		this.queueMaxMsgRedelivery = queueMaxMsgRedelivery;
	}

	public Integer getQueueMaxMsgSize() {
		return queueMaxMsgSize;
	}

	public void setQueueMaxMsgSize(Integer queueMaxMsgSize) {
		this.queueMaxMsgSize = queueMaxMsgSize;
	}

	public Integer getQueueQuota() {
		return queueQuota;
	}

	public void setQueueQuota(Integer queueQuota) {
		this.queueQuota = queueQuota;
	}

	public Boolean getQueueRespectsMsgTtl() {
		return queueRespectsMsgTtl;
	}

	public void setQueueRespectsMsgTtl(Boolean queueRespectsMsgTtl) {
		this.queueRespectsMsgTtl = queueRespectsMsgTtl;
	}
}
