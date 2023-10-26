package com.solace.spring.cloud.stream.binder.health;

import com.solace.spring.cloud.stream.binder.health.contributors.BindingHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.FlowsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.SolaceBinderHealthContributor;
import com.solace.spring.cloud.stream.binder.health.handlers.SolaceFlowHealthEventHandler;
import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;

import java.util.Optional;

/**
 * <p>Proxy class for the Solace binder to access health components.
 * Always use this instead of directly using health components in Solace binder code.</p>
 * <p>Allows for the Solace binder to still function correctly without actuator on the classpath.</p>
 */
public class SolaceBinderHealthAccessor {
	private final SolaceBinderHealthContributor solaceBinderHealthContributor;
	private static final String FLOW_ID_CONCURRENCY_IDX_PREFIX = "flow-";

	public SolaceBinderHealthAccessor(SolaceBinderHealthContributor solaceBinderHealthContributor) {
		this.solaceBinderHealthContributor = solaceBinderHealthContributor;
	}

	public void addFlow(String bindingName, int concurrencyIdx, FlowReceiverContainer flowReceiverContainer) {
		FlowHealthIndicator flowHealthIndicator = new FlowHealthIndicator();
		Optional.ofNullable(solaceBinderHealthContributor.getSolaceBindingsHealthContributor())
				.map(b -> b.getContributor(bindingName))
				.orElseGet(() -> {
					BindingHealthContributor newBindingHealth = new BindingHealthContributor(new FlowsHealthContributor());
					solaceBinderHealthContributor.getSolaceBindingsHealthContributor()
							.addBindingContributor(bindingName, newBindingHealth);
					return newBindingHealth;
				})
				.getFlowsHealthContributor()
				.addFlowContributor(createFlowIdFromConcurrencyIdx(concurrencyIdx), flowHealthIndicator);
		flowReceiverContainer.setEventHandler(new SolaceFlowHealthEventHandler(
				flowReceiverContainer.getXMLMessageMapper(),
				flowReceiverContainer.getId().toString(),
				flowHealthIndicator));
	}

	public void removeFlow(String bindingName, int concurrencyIdx) {
		Optional.ofNullable(solaceBinderHealthContributor.getSolaceBindingsHealthContributor())
				.map(b -> b.getContributor(bindingName))
				.map(BindingHealthContributor::getFlowsHealthContributor)
				.ifPresent(b -> b.removeFlowContributor(createFlowIdFromConcurrencyIdx(concurrencyIdx)));
	}

	private String createFlowIdFromConcurrencyIdx(int concurrencyIdx) {
		return FLOW_ID_CONCURRENCY_IDX_PREFIX + concurrencyIdx;
	}
}
