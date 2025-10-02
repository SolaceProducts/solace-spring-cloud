package com.solace.spring.cloud.stream.binder.util;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;

public interface SolaceSessionManager {

  JCSMPSession getSession() throws JCSMPException;
}