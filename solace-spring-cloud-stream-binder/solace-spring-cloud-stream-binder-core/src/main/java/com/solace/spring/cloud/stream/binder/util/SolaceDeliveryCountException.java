package com.solace.spring.cloud.stream.binder.util;

public class SolaceDeliveryCountException extends RuntimeException {

    public SolaceDeliveryCountException(Throwable throwable) {
        super(throwable);
    }
}
