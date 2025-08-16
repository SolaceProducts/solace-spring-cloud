package com.solace.spring.cloud.stream.binder.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("solace.java")
public class SolaceExtendedJavaProperties {

	private boolean startupFailOnConnectError = true;

	public boolean isStartupFailOnConnectError() {
		return startupFailOnConnectError;
	}

	public void setStartupFailOnConnectError(boolean startupFailOnConnectError) {
		this.startupFailOnConnectError = startupFailOnConnectError;
	}
}
