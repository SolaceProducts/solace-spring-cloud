package com.solace.spring.cloud.stream.binder.test.util;

import java.io.Serializable;

public record SerializableFoo(String foo) implements Serializable {
}
