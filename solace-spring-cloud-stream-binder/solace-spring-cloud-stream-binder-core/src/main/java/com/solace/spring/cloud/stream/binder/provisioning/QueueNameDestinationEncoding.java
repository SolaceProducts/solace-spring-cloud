package com.solace.spring.cloud.stream.binder.provisioning;

enum QueueNameDestinationEncoding {
	PLAIN("plain");
	private final String label;

	QueueNameDestinationEncoding(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}
}
