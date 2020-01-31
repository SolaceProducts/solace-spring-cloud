package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredSpringRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceExternalResourceHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;

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

	JCSMPSession jcsmpSession;

	static SolaceExternalResourceHandler externalResource = new SolaceExternalResourceHandler();


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
		if (testBinder == null) {
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
