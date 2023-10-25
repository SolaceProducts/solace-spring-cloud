package com.solace.spring.cloud.stream.binder.health;

import com.solace.spring.cloud.stream.binder.health.contributors.BindingHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.BindingsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.FlowsHealthContributor;
import com.solace.spring.cloud.stream.binder.health.contributors.SolaceBinderHealthContributor;
import com.solace.spring.cloud.stream.binder.health.indicators.FlowHealthIndicator;
import com.solace.spring.cloud.stream.binder.health.indicators.SessionHealthIndicator;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.NamedContributor;

import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class SolaceBinderHealthAccessorTest {
	@CartesianTest(name = "[{index}] bindingHealthContributorExists={0} flowHealthExists={1}")
	public void testAddFlow(@CartesianTest.Values(booleans = {false, true}) boolean bindingHealthContributorExists,
							@CartesianTest.Values(booleans = {false, true}) boolean flowHealthExists,
							@Mock FlowReceiverContainer flowReceiverContainer) {
		Mockito.when(flowReceiverContainer.getId()).thenReturn(UUID.randomUUID());
		SolaceBinderHealthContributor healthContributor = new SolaceBinderHealthContributor(
				new SessionHealthIndicator(),
				new BindingsHealthContributor());
		SolaceBinderHealthAccessor healthAccessor = new SolaceBinderHealthAccessor(healthContributor);

		String bindingName = "binding-name";
		int concurrencyIdx = 55;

		if (bindingHealthContributorExists) {
			FlowsHealthContributor flowsHealthContributor = new FlowsHealthContributor();
			if (flowHealthExists) {
				flowsHealthContributor.addFlowContributor("flow-" + concurrencyIdx, new FlowHealthIndicator());
			}
			healthContributor.getSolaceBindingsHealthContributor()
					.addBindingContributor(bindingName, new BindingHealthContributor(flowsHealthContributor));
		}

		healthAccessor.addFlow(bindingName, concurrencyIdx, flowReceiverContainer);

		assertThat(StreamSupport.stream(healthContributor.getSolaceBindingsHealthContributor().spliterator(), false))
				.singleElement()
				.satisfies(n -> assertThat(n.getName()).isEqualTo(bindingName))
				.extracting(NamedContributor::getContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(BindingHealthContributor.class))
				.extracting(BindingHealthContributor::getFlowsHealthContributor)
				.asInstanceOf(InstanceOfAssertFactories.type(FlowsHealthContributor.class))
				.extracting(c -> StreamSupport.stream(c.spliterator(), false))
				.asInstanceOf(InstanceOfAssertFactories.stream(NamedContributor.class))
				.singleElement()
				.satisfies(n -> assertThat(n.getName()).isEqualTo("flow-" + concurrencyIdx))
				.extracting(NamedContributor::getContributor)
				.isInstanceOf(FlowHealthIndicator.class);
	}

	@CartesianTest(name = "[{index}] bindingHealthContributorExists={0} flowHealthExists={1}")
	public void testRemoveFlow(
			@CartesianTest.Values(booleans = {false, true}) boolean bindingHealthContributorExists,
			@CartesianTest.Values(booleans = {false, true}) boolean flowHealthExists) {
		SolaceBinderHealthContributor healthContributor = new SolaceBinderHealthContributor(
				new SessionHealthIndicator(),
				new BindingsHealthContributor());
		SolaceBinderHealthAccessor healthAccessor = new SolaceBinderHealthAccessor(healthContributor);

		String bindingName = "binding-name";
		int concurrencyIdx = 55;

		if (bindingHealthContributorExists) {
			FlowsHealthContributor flowsHealthContributor = new FlowsHealthContributor();
			if (flowHealthExists) {
				flowsHealthContributor.addFlowContributor("flow-" + concurrencyIdx, new FlowHealthIndicator());
			}
			healthContributor.getSolaceBindingsHealthContributor()
					.addBindingContributor(bindingName, new BindingHealthContributor(flowsHealthContributor));
		}

		healthAccessor.removeFlow(bindingName, concurrencyIdx);

		if (bindingHealthContributorExists) {
			assertThat(StreamSupport.stream(healthContributor.getSolaceBindingsHealthContributor().spliterator(), false))
					.singleElement()
					.satisfies(n -> assertThat(n.getName()).isEqualTo(bindingName))
					.extracting(NamedContributor::getContributor)
					.asInstanceOf(InstanceOfAssertFactories.type(BindingHealthContributor.class))
					.extracting(BindingHealthContributor::getFlowsHealthContributor)
					.asInstanceOf(InstanceOfAssertFactories.type(FlowsHealthContributor.class))
					.extracting(c -> StreamSupport.stream(c.spliterator(), false))
					.asInstanceOf(InstanceOfAssertFactories.stream(NamedContributor.class))
					.isEmpty();
		} else {
			assertThat(StreamSupport.stream(healthContributor.getSolaceBindingsHealthContributor().spliterator(), false))
					.isEmpty();
		}
	}
}
