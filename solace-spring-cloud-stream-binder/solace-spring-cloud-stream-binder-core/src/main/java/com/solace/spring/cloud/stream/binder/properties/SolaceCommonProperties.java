package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;

public class SolaceCommonProperties {
	private boolean provisionDurableQueue = true;
	@Deprecated
	private boolean provisionSubscriptionsToDurableQueue = true;
	/**
	 * Whether to add the destination topic as a queue subscription during provisioning.
	 */
	private boolean addDestinationAsSubscriptionToQueue = true;

	// Queue Properties -------
	private String queueNamePrefix = "scst";
	private boolean useFamiliarityInQueueName = true;
	private boolean useDestinationEncodingInQueueName = true;

	private int queueAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	private int queuePermission = EndpointProperties.PERMISSION_CONSUME;
	private Integer queueDiscardBehaviour = null;
	private Integer queueMaxMsgRedelivery = null;
	private Integer queueMaxMsgSize = null;
	private Integer queueQuota = null;
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
