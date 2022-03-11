package com.solace.spring.cloud.stream.binder.test.junit.launcher.filter;

import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import org.junit.jupiter.engine.descriptor.ClassTestDescriptor;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Used by the JUnit Jupiter Engine to statically filter tests and test classes.
 */
public class PostDiscoveryClassNameExclusionFilter implements PostDiscoveryFilter {
	private static final Logger LOGGER = LoggerFactory.getLogger(PostDiscoveryClassNameExclusionFilter.class);

	@Override
	public FilterResult apply(final TestDescriptor object) {
		if (excludeByTestClass(object, SpringCloudStreamContext.class)) {
			return FilterResult.excluded("Skip tests ran from " + SpringCloudStreamContext.class.getSimpleName());
		} else {
			return FilterResult.included("Is a valid test descriptor");
		}
	}

	private boolean excludeByTestClass(final TestDescriptor object, Class<?>... excludeClasses) {
		for (TestDescriptor testDescriptor = object;
			 testDescriptor != null;
			 testDescriptor = testDescriptor.getParent().orElse(null)) {

			if (testDescriptor instanceof ClassTestDescriptor) {
				ClassTestDescriptor classTestDescriptor = (ClassTestDescriptor) testDescriptor;
				if (Arrays.asList(excludeClasses).contains(classTestDescriptor.getTestClass())) {
					LOGGER.debug("Excluding test descriptor: {}", classTestDescriptor);
					return true;
				}
			}
		}
		return false;
	}
}
