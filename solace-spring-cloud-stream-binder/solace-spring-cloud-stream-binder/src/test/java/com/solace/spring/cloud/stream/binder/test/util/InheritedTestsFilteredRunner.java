package com.solace.spring.cloud.stream.binder.test.util;

import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.parameterized.BlockJUnit4ClassRunnerWithParameters;
import org.junit.runners.parameterized.ParametersRunnerFactory;
import org.junit.runners.parameterized.TestWithParameters;

/**
 * <p>A JUnit runner which filters out all inherited tests if the test class is annotated with
 * {@link IgnoreInheritedTests IgnoreInheritedTests}.
 *
 * <p>Based off this Stack Overflow answer:
 * https://stackoverflow.com/questions/2207070/junit-ignore-test-methods-from-base-class/5248700#answer-12459947
 */
public final class InheritedTestsFilteredRunner extends BlockJUnit4ClassRunner {
	public InheritedTestsFilteredRunner(Class<?> klass) throws InitializationError {
		super(klass);
		try {
			this.filter(new InheritedTestsFilter());
		} catch (NoTestsRemainException e) {
			throw new IllegalStateException("class should contain at least one runnable test", e);
		}
	}

	/**
	 * <p>The Parameterized variant of this runner.
	 *
	 * <p>Based off of this Stack Overflow answer:
	 * https://stackoverflow.com/questions/27745691/how-to-combine-runwith-with-runwithparameterized-class
	 */
	public static class ParameterizedRunner extends BlockJUnit4ClassRunnerWithParameters {
		public ParameterizedRunner(TestWithParameters test) throws InitializationError {
			super(test);
			try {
				this.filter(new InheritedTestsFilter());
			} catch (NoTestsRemainException e) {
				throw new IllegalStateException("class should contain at least one runnable test", e);
			}
		}
	}

	public static class ParameterizedRunnerFactory implements ParametersRunnerFactory {

		@Override
		public Runner createRunnerForTestWithParameters(TestWithParameters test) throws InitializationError {
			return new ParameterizedRunner(test);
		}
	}

	public static class InheritedTestsFilter extends Filter {
		@Override
		public boolean shouldRun(Description description) {
			Class<?> clazz = description.getTestClass();
			String methodName = description.getMethodName();
			if (methodName.contains("[")) { // Parameterized tests have this set to methodName[ParametersName]
				methodName = methodName.substring(0, methodName.indexOf("["));
			}

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
