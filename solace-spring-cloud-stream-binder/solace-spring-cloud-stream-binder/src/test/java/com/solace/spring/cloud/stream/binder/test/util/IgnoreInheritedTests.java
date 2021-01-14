package com.solace.spring.cloud.stream.binder.test.util;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * When the test class is marked with {@link IgnoreInheritedTests @IgnoreInheritedTests}
 * and runs with the {@link InheritedTestsFilteredRunner InheritedTestsFilteredSpringRunner},
 * all inherited test cases will be filtered from the test run.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface IgnoreInheritedTests {
}
