package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredSpringRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceExternalResourceHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * <p>Base class for all Solace Spring Cloud Stream Binder test classes.
 *
 * <p>Typically, you'll want to filter out all inherited test cases from
 * the parent class {@link PartitionCapableBinderTests PartitionCapableBinderTests}.
 * To do this, run your test class with the
 * {@link InheritedTestsFilteredSpringRunner InheritedTestsFilteredSpringRunner} runner
 * along with the {@link IgnoreInheritedTests @IgnoreInheritedTests} annotation.
 */
public abstract class SolaceBinderTestBase
		extends PartitionCapableBinderTests<SolaceTestBinder, ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>> {
	@Autowired
	private SpringJCSMPFactory springJCSMPFactory;

	@Value("${test.fail.on.connection.exception:false}")
	private Boolean failOnConnectError;

	@Value("${test.solace.mgmt.host:#{null}}")
	private String solaceMgmtHost;

	@Value("${test.solace.mgmt.username:#{null}}")
	private String solaceMgmtUsername;

	@Value("${test.solace.mgmt.password:#{null}}")
	private String solaceMgmtPassword;

	JCSMPSession jcsmpSession;
	SempV2Api sempV2Api;

	static SolaceExternalResourceHandler externalResource = new SolaceExternalResourceHandler();

	@Before
	public void setupSempV2Api() {
		if (failOnConnectError) {
			assertThat(solaceMgmtHost).as("test.solace.mgmt.host cannot be blank").isNotBlank();
			assertThat(solaceMgmtUsername).as("test.solace.mgmt.username cannot be blank").isNotBlank();
			assertThat(solaceMgmtPassword).as("test.solace.mgmt.password cannot be blank").isNotBlank();
		} else {
			assumeThat(solaceMgmtHost).as("test.solace.mgmt.host cannot be blank. Skipping test...").isNotBlank();
			assumeThat(solaceMgmtUsername).as("test.solace.mgmt.username cannot be blank. Skipping test...").isNotBlank();
			assumeThat(solaceMgmtPassword).as("test.solace.mgmt.password cannot be blank. Skipping test...").isNotBlank();
		}
		sempV2Api = new SempV2Api(solaceMgmtHost, solaceMgmtUsername, solaceMgmtPassword);
	}


	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Override
	protected String getClassUnderTestName() {
		return this.getClass().getSimpleName();
	}

	@Override
	protected SolaceTestBinder getBinder() throws Exception {
		if (testBinder == null || jcsmpSession.isClosed()) {
			if (testBinder != null) {
				logger.info(String.format("Will recreate %s since %s is closed",
						testBinder.getClass().getSimpleName(), jcsmpSession.getClass().getSimpleName()));
				testBinder.getBinder().destroy();
				testBinder = null;
			}

			logger.info(String.format("Getting new %s instance", SolaceTestBinder.class.getSimpleName()));
			jcsmpSession = externalResource.assumeAndGetActiveSession(springJCSMPFactory, failOnConnectError);
			testBinder = new SolaceTestBinder(jcsmpSession);
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<SolaceProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new SolaceProducerProperties());
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}
}
