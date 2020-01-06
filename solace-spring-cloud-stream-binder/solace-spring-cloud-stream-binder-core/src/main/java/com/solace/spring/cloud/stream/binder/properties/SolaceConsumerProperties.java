package com.solace.spring.cloud.stream.binder.properties;

import com.solacesystems.jcsmp.EndpointProperties;

public class SolaceConsumerProperties extends SolaceCommonProperties {
	private String anonymousGroupPostfix = "anon";
	private int polledConsumerWaitTimeInMillis = 100;
	private boolean requeueRejected = false;

	private String[] queueAdditionalSubscriptions = new String[0];

	// DMQ Properties ---------
	private boolean autoBindDmq = false;
	private int dmqAccessType = EndpointProperties.ACCESSTYPE_NONEXCLUSIVE;
	private int dmqPermission = EndpointProperties.PERMISSION_CONSUME;
	private Integer dmqDiscardBehaviour = null;
	private Integer dmqMaxMsgRedelivery = null;
	private Integer dmqMaxMsgSize = null;
	private Integer dmqQuota = null;
	private Boolean dmqRespectsMsgTtl = null;
	private Long republishedMsgTtl = null;
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

	public boolean isRequeueRejected() {
		return requeueRejected;
	}

	public void setRequeueRejected(boolean requeueRejected) {
		this.requeueRejected = requeueRejected;
	}

	public String[] getQueueAdditionalSubscriptions() {
		return queueAdditionalSubscriptions;
	}

	public void setQueueAdditionalSubscriptions(String[] queueAdditionalSubscriptions) {
		this.queueAdditionalSubscriptions = queueAdditionalSubscriptions;
	}

	public boolean isAutoBindDmq() {
		return autoBindDmq;
	}

	public void setAutoBindDmq(boolean autoBindDmq) {
		this.autoBindDmq = autoBindDmq;
	}

	public int getDmqAccessType() {
		return dmqAccessType;
	}

	public void setDmqAccessType(int dmqAccessType) {
		this.dmqAccessType = dmqAccessType;
	}

	public int getDmqPermission() {
		return dmqPermission;
	}

	public void setDmqPermission(int dmqPermission) {
		this.dmqPermission = dmqPermission;
	}

	public Integer getDmqDiscardBehaviour() {
		return dmqDiscardBehaviour;
	}

	public void setDmqDiscardBehaviour(Integer dmqDiscardBehaviour) {
		this.dmqDiscardBehaviour = dmqDiscardBehaviour;
	}

	public Integer getDmqMaxMsgRedelivery() {
		return dmqMaxMsgRedelivery;
	}

	public void setDmqMaxMsgRedelivery(Integer dmqMaxMsgRedelivery) {
		this.dmqMaxMsgRedelivery = dmqMaxMsgRedelivery;
	}

	public Integer getDmqMaxMsgSize() {
		return dmqMaxMsgSize;
	}

	public void setDmqMaxMsgSize(Integer dmqMaxMsgSize) {
		this.dmqMaxMsgSize = dmqMaxMsgSize;
	}

	public Integer getDmqQuota() {
		return dmqQuota;
	}

	public void setDmqQuota(Integer dmqQuota) {
		this.dmqQuota = dmqQuota;
	}

	public Boolean getDmqRespectsMsgTtl() {
		return dmqRespectsMsgTtl;
	}

	public void setDmqRespectsMsgTtl(Boolean dmqRespectsMsgTtl) {
		this.dmqRespectsMsgTtl = dmqRespectsMsgTtl;
	}

	public Long getRepublishedMsgTtl() {
		return republishedMsgTtl;
	}

	public void setRepublishedMsgTtl(Long republishedMsgTtl) {
		this.republishedMsgTtl = republishedMsgTtl;
	}
}
