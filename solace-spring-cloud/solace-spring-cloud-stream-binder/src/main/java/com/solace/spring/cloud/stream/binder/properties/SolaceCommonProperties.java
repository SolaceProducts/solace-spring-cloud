package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;

public class SolaceCommonProperties {
	private String prefix = ""; // Naming prefix for all topics and queues

	// Queue Properties -------
	private int queueAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	private int queuePermission = EndpointProperties.PERMISSION_CONSUME;
	private Integer queueDiscardBehaviour = null;
	private Integer queueMaxMsgRedelivery = null;
	private Integer queueMaxMsgSize = null;
	private Integer queueQuota = null;
	private Boolean queueRespectsMsgTtl = null;
	// ------------------------

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String prefix) {
		this.prefix = prefix;
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
