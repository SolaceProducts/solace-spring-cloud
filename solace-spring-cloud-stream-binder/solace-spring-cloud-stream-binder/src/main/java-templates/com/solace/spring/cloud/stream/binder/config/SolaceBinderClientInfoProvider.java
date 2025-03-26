package com.solace.spring.cloud.stream.binder.config;

import com.solacesystems.jcsmp.impl.client.ClientInfoProvider;

public class SolaceBinderClientInfoProvider extends ClientInfoProvider {

	public static final String VERSION = "@project.version@";

	public String getSoftwareVersion() {
		return String.format("%s (%s)", VERSION,  super.getSoftwareVersion());
	}

	public String getSoftwareDate() {
		return String.format("@build.timestamp@ (%s)", super.getSoftwareDate());
	}

	public String getPlatform() {
		return this.getPlatform("@project.name@ (JCSMP SDK)");
	}
}
