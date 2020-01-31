package com.solace.spring.cloud.stream.binder.test.util;

import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.model.InitializationError;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * <p>Extends off of the {@link SpringJUnit4ClassRunner SpringJUnit4ClassRunner},
 * but with the additional feature of filtering out all inherited tests
 * if the test class is annotated with {@link IgnoreInheritedTests IgnoreInheritedTests}.
 *
 * <p>Based off this Stack Overflow answer:
 * https://stackoverflow.com/questions/2207070/junit-ignore-test-methods-from-base-class/5248700#answer-12459947
 */
public final class InheritedTestsFilteredSpringRunner extends SpringJUnit4ClassRunner {
	/**
	 * Construct a new {@code SpringRunner} and initialize a
	 * {@link TestContextManager TestContextManager}
	 * to provide Spring testing functionality to standard JUnit 4 tests.
	 *
	 * Also filters out any inherited JUnit test methods if the test class is also annotated with
	 * {@link IgnoreInheritedTests IgnoreInheritedTests}.
	 *
	 * @param clazz the test class to be run
	 * @see #createTestContextManager(Class)
	 */
	public InheritedTestsFilteredSpringRunner(Class<?> clazz) throws InitializationError {
		super(clazz);
		try {
			this.filter(new InheritedTestsFilter());
		} catch (NoTestsRemainException e) {
			throw new IllegalStateException("class should contain at least one runnable test", e);
		}
	}

	public static class InheritedTestsFilter extends Filter {

		@Override
		public boolean shouldRun(Description description) {
			Class<?> clazz = description.getTestClass();
			String methodName = description.getMethodName();
			if (clazz.isAnnotationPresent(IgnoreInheritedTests.class)) {
				try {
					return clazz.getDeclaredMethod(methodName) != null;
				} catch (Exception e) {
					return false;
				}
			}
			return true;
		}

		@Override
		public String describe() {
			return null;
		}

	}
}
