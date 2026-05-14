package com.solace.spring.cloud.stream.binder.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

abstract sealed class SharedResourceManager<T> permits JCSMPSessionProducerManager {
	private final String type;
	T sharedResource;
	private Set<String> registeredIds = new HashSet<>();
	private final Object lock = new Object();

	private static final Logger LOGGER = LoggerFactory.getLogger(SharedResourceManager.class);

	SharedResourceManager(String type) {
		this.type = type;
	}

	abstract T create() throws Exception;
	abstract void close();

	/**
	 * Register {@code key} to the shared resource.
	 * <p>If this is the first key to claim this shared resource, {@link #create()} the resource.
	 * @param key the registration key of the caller that wants to use this resource
	 * @return the shared resource
	 * @throws Exception whatever exception that may be thrown by {@link #create()}
	 */
	public T get(String key) throws Exception {
		synchronized (lock) {
			if (registeredIds.isEmpty()) {
				LOGGER.info("No {} exists, a new one will be created", type);
				sharedResource = create();
			} else {
				LOGGER.trace("A message {} already exists, reusing it", type);
			}

			registeredIds.add(key);
		}

		return sharedResource;
	}

	/**
	 * Conditionally replace the shared resource using compare-and-swap semantics.
	 *
	 * <p>If the manager still holds the {@code expected} reference, the existing
	 * resource is closed and a fresh one is {@link #create()}d. If the manager
	 * already holds a different reference - because a concurrent caller has already
	 * replaced it - this is a no-op and the currently-installed resource is
	 * returned. This prevents two callers that observed the same stale resource
	 * from both recreating: the second caller sees that the resource has already
	 * changed and uses the replacement rather than closing a potentially in-use
	 * resource that the first caller installed.
	 *
	 * <p>Existing registrations are preserved, so subsequent {@link #get(String)}
	 * calls from any registered caller return the (possibly newly-installed)
	 * resource.
	 *
	 * @param expected the resource reference the caller observed and considers no
	 *                 longer usable; pass the value previously returned by
	 *                 {@link #get(String)} or by an earlier call to this method
	 * @return the resource currently installed in the manager - either the
	 *         freshly-created one (if the swap happened) or whatever a concurrent
	 *         caller installed (if it did not)
	 * @throws Exception whatever exception may be thrown by {@link #create()}
	 */
	public T forceRecreate(T expected) throws Exception {
		synchronized (lock) {
			if (sharedResource != expected) {
				return sharedResource;
			}
			if (sharedResource != null) {
				try {
					close();
				} catch (Exception e) {
					LOGGER.debug("Failed to close current {} during forceRecreate", type, e);
				}
			}
			sharedResource = create();
			return sharedResource;
		}
	}

	/**
	 * De-register {@code key} from the shared resource.
	 * <p>If this is the last {@code key} associated to the shared resource, {@link #close()} the resource.
	 * @param key the registration key of the caller that is using the resource
	 */
	public void release(String key) {
		synchronized (lock) {
			if (!registeredIds.contains(key)) return;

			if (registeredIds.size() <= 1) {
				LOGGER.info("{} is the last user, closing {}...", key, type);
				close();
				sharedResource = null;
			} else {
				LOGGER.info("{} is not the last user, persisting {}...", key, type);
			}
			registeredIds.remove(key);
		}
	}
}
