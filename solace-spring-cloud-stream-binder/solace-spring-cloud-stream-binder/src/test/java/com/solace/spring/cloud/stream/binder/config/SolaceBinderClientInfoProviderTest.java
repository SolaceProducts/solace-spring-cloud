package com.solace.spring.cloud.stream.binder.config;

import org.junit.Test;

import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceBinderClientInfoProviderTest {
	@Test
	public void testSoftwareVersion() {
		Pattern versionPattern = Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+");
		Pattern pattern = Pattern.compile(String.format("%s(?:-SNAPSHOT)? \\(%s\\)", versionPattern, versionPattern));
		assertThat(new SolaceBinderClientInfoProvider().getSoftwareVersion()).matches(pattern);
	}

	@Test
	public void testSoftwareDate() {
		Pattern datePattern = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");
		Pattern pattern = Pattern.compile(String.format("%s \\(%s\\)", datePattern, datePattern));
		assertThat(new SolaceBinderClientInfoProvider().getSoftwareDate()).matches(pattern);
	}

	@Test
	public void testPlatform() {
		String platformPostfix = "Solace Spring Cloud Stream Binder (JCSMP SDK)";
		assertThat(new SolaceBinderClientInfoProvider().getPlatform()).endsWith(platformPostfix);
	}
}
