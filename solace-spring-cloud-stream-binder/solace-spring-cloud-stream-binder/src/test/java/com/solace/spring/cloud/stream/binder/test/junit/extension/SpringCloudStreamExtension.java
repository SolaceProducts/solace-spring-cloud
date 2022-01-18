package com.solace.spring.cloud.stream.binder.test.junit.extension;

import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

public class SpringCloudStreamExtension implements AfterEachCallback, BeforeEachCallback, ParameterResolver {
	private static final Namespace NAMESPACE = Namespace.create(SpringCloudStreamContext.class);
	private final PubSubPlusExtension pubSubPlusExtension;

	public SpringCloudStreamExtension(PubSubPlusExtension pubSubPlusExtension) {
		this.pubSubPlusExtension = pubSubPlusExtension;
	}

	@Override
	public void afterEach(ExtensionContext context) {
		SpringCloudStreamContext cloudStreamContext = context.getStore(NAMESPACE)
				.get(SpringCloudStreamContext.class, SpringCloudStreamContext.class);
		if (cloudStreamContext != null) {
			cloudStreamContext.cleanup();
		}
	}

	@Override
	public void beforeEach(ExtensionContext context) {
		SpringCloudStreamContext cloudStreamContext = context.getStore(NAMESPACE)
				.get(SpringCloudStreamContext.class, SpringCloudStreamContext.class);
		if (cloudStreamContext != null) {
			cloudStreamContext.before();
		}
	}

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return SpringCloudStreamContext.class.isAssignableFrom(parameterContext.getParameter().getType());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		Class<?> paramType = parameterContext.getParameter().getType();
		if (SpringCloudStreamContext.class.isAssignableFrom(paramType)) {
			return extensionContext.getStore(NAMESPACE).getOrComputeIfAbsent(SpringCloudStreamContext.class,
					c -> {
						SpringCloudStreamContext context = new SpringCloudStreamContext(
								pubSubPlusExtension.getJCSMPSession(extensionContext));
						context.before();
						return context;
					},
					SpringCloudStreamContext.class);
		} else {
			throw new ParameterResolutionException("Cannot resolve parameter type " + paramType.getSimpleName());
		}
	}
}
