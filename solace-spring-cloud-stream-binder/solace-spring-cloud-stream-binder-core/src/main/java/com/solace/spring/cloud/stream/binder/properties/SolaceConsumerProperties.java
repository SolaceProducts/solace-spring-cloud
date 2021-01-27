package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;

public class SolaceConsumerProperties extends SolaceCommonProperties {
	private String anonymousGroupPostfix = "anon";
	private int polledConsumerWaitTimeInMillis = 100;

	private String[] queueAdditionalSubscriptions = new String[0];
	private boolean useGroupNameInQueueName = true;

	// Error Queue Properties ---------
	private boolean autoBindErrorQueue = false;
	private boolean provisionErrorQueue = true;
	private boolean useGroupNameInErrorQueueName = true;

	private int errorQueueAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	private int errorQueuePermission = EndpointProperties.PERMISSION_CONSUME;
	private Integer errorQueueDiscardBehaviour = null;
	private Integer errorQueueMaxMsgRedelivery = null;
	private Integer errorQueueMaxMsgSize = null;
	private Integer errorQueueQuota = null;
	private Boolean errorQueueRespectsMsgTtl = null;

	private Boolean errorMsgDmqEligible = null;
	private Long errorMsgTtl = null;
	// ------------------------

	public String getAnonymousGroupPostfix() {
		return anonymousGroupPostfix;
	}

	public void setAnonymousGroupPostfix(String anonymousGroupPostfix) {
		this.anonymousGroupPostfix = anonymousGroupPostfix;
	}

	public int getPolledConsumerWaitTimeInMillis() {
		return polledConsumerWaitTimeInMillis;
	}

	public void setPolledConsumerWaitTimeInMillis(int polledConsumerWaitTimeInMillis) {
		this.polledConsumerWaitTimeInMillis = polledConsumerWaitTimeInMillis;
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

	public boolean isUseGroupNameInErrorQueueName() {
		return useGroupNameInErrorQueueName;
	}

	public void setUseGroupNameInErrorQueueName(boolean useGroupNameInErrorQueueName) {
		this.useGroupNameInErrorQueueName = useGroupNameInErrorQueueName;
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
