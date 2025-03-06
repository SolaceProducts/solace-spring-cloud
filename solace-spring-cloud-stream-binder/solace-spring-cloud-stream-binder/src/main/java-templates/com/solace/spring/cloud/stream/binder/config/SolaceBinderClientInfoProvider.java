package com.solace.spring.cloud.stream.binder.config;

import com.solacesystems.jcsmp.impl.client.ClientInfoProvider;

public class SolaceBinderClientInfoProvider extends ClientInfoProvider {
	public String getSoftwareVersion() {
		return String.format("@project.version@ (%s)", super.getSoftwareVersion());
	}

	public String getSoftwareDate() {
		return String.format("@build.timestamp@ (%s)", super.getSoftwareDate());
	}

	public String getPlatform() {
		return this.getPlatform("@project.name@ (JCSMP SDK)");
	}
}
