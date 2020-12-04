package com.solace.spring.cloud.stream.binder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class ITBase {
	@ClassRule
	public static final SpringClassRule springClassRule = new SpringClassRule();

	@Rule
	public final SpringMethodRule springMethodRule = new SpringMethodRule();

	@Autowired
	private SpringJCSMPFactory springJCSMPFactory;

	@Value("${test.solace.mgmt.host:#{null}}")
	private String solaceMgmtHost;

	@Value("${test.solace.mgmt.username:#{null}}")
	private String solaceMgmtUsername;

	@Value("${test.solace.mgmt.password:#{null}}")
	private String solaceMgmtPassword;

	protected JCSMPSession jcsmpSession;
	protected SempV2Api sempV2Api;
	protected ObjectMapper jsonMapper;

	@Before
	public void setupSempV2Api() {
		assertThat(solaceMgmtHost).as("test.solace.mgmt.host cannot be blank").isNotBlank();
		assertThat(solaceMgmtUsername).as("test.solace.mgmt.username cannot be blank").isNotBlank();
		assertThat(solaceMgmtPassword).as("test.solace.mgmt.password cannot be blank").isNotBlank();
		sempV2Api = new SempV2Api(solaceMgmtHost, solaceMgmtUsername, solaceMgmtPassword);
	}

	@Before
	public void createJcsmpSession() throws Exception {
		jcsmpSession = springJCSMPFactory.createSession();
		jcsmpSession.connect();
	}

	@After
	public void closeJcsmpSession() {
		if (jcsmpSession != null && !jcsmpSession.isClosed()) {
			jcsmpSession.closeSession();
		}
	}
}
