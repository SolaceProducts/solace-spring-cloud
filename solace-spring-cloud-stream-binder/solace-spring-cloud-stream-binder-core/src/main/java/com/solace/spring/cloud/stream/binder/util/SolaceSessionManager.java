package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;

/**
 * Interface for managing Solace JCSMP sessions within the Spring Cloud Stream binder.
 */
public interface SolaceSessionManager {

  /**
   * Create a single shared JCSMP session for Solace messaging operations.
   *
   * @return the active JCSMP session
   * @throws JCSMPException if the session cannot be retrieved or created
   */
  JCSMPSession getSession() throws JCSMPException;

  /**
   * Closes the session manager and releases all associated resources.
   */
  void close();
}