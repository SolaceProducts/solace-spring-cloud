package com.solace.spring.cloud.stream.binder.messaging;

import com.solace.spring.cloud.stream.binder.test.junit.param.provider.SolaceSpringHeaderArgumentsProvider;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalToObject;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.CoreMatchers.startsWithIgnoringCase;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SolaceHeadersTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SolaceHeadersTest.class);

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.ClassesOnly.class)
	public void testPrefix(Class<?> headersClass) throws Exception {
		Field field = getPrefixField(headersClass);
		assertEquals(String.class, field.getType(), String.format("%s is not a String", field.getName()));
		assertTrue(Modifier.isStatic(field.getModifiers()), String.format("%s is not static", field.getName()));
		assertTrue(Modifier.isFinal(field.getModifiers()), String.format("%s is not final", field.getName()));
		assertThat((String) field.get(null), startsWith(SolaceHeaders.PREFIX));
		assertTrue(((String) field.get(null)).matches("[a-z][a-z_]+_"));

		if (headersClass != SolaceHeaders.class) {
			assertNotEquals(field.get(null), SolaceHeaders.PREFIX);
		}
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.ClassesOnly.class)
	public void testFieldDeclaration(Class<?> headersClass) {
		for (Field field : getAllHeaderFields(headersClass)) {
			assertEquals(String.class, field.getType(), String.format("%s is not a String", field.getName()));
			assertTrue(Modifier.isFinal(field.getModifiers()), String.format("%s is not final", field.getName()));
		}
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.ClassesOnly.class)
	public void testFieldNameSyntax(Class<?> headersClass) throws Exception {
		for (Field field : getAllHeaderFields(headersClass)) {
			assertTrue(field.getName().matches("[A-Z][A-Z_]+[A-Z]"),
					String.format("%s name does not have proper syntax", field.getName()));

			assertThat(String.format("%s name should not start with prefix", field.getName()),
					field.getName(), not(startsWithIgnoringCase((String) getPrefixField(headersClass).get(null))));

			String noPrefixHeader = ((String) field.get(null))
					.substring(((String) getPrefixField(headersClass).get(null)).length());
			assertEquals(camelCaseToSnakeCase(noPrefixHeader).toUpperCase(), field.getName(), String.format(
					"%s name should be the prefix-trimmed, fully-capitalized, '_'-delimited version of %s",
					field.getName(), field.get(null)));
		}
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.ClassesOnly.class)
	public void testHeaderSyntax(Class<?> headersClass) throws Exception {
		for (String header : getAllHeaders(headersClass)) {
			String prefix = (String) getPrefixField(headersClass).get(null);
			assertThat(header, startsWith(prefix));
			assertTrue(header.matches(prefix + "[a-z][a-zA-Z]+"),
					String.format("%s does not have proper syntax", header));
		}
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.ClassesOnly.class)
	public void testUniqueHeaders(Class<?> headersClass) {
		List<String> headers = getAllHeaders(headersClass);
		assertEquals(headers.stream().distinct().count(), headers.size(),
				String.join(", ", headers) + " does not have unique values");
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testHeadersHaveMetaObjects(Class<?> headersClass, Map<String, ? extends HeaderMeta<?>> headersMeta) {
		List<String> headers = getAllHeaders(headersClass);
		assertEquals(headersMeta.size(), headers.size());
		for (String header : headers) {
			assertThat(headersMeta.keySet(), hasItem(header));
		}
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.MetaOnly.class)
	public void testValidMeta(Map<String, ? extends HeaderMeta<?>> headersMeta) {
		headersMeta.values()
				.forEach(m -> {
					assertNotNull(m.getType());
					assertFalse(m.getType().isPrimitive(),
							String.format("primitives not supported by %s", MessageHeaders.class.getSimpleName()));
				});
	}

	@ParameterizedTest
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.MetaOnly.class)
	public void testUniqueMetaNames(Map<String, ? extends HeaderMeta<?>> headersMeta) {
		assertEquals(headersMeta.keySet().size(), headersMeta.keySet().stream().distinct().count(),
				String.join(", ", headersMeta.keySet()) + " does not have unique values");
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testMetaReadActions(Class<?> headersClass, Map<String, ? extends HeaderMeta<?>> headersMeta) {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			LOGGER.info("Test does not apply to {}", headersClass.getSimpleName());
			return;
		}

		XMLMessage xmlMessage = Mockito.spy(JCSMPFactory.onlyInstance().createMessage(TextMessage.class));
		@SuppressWarnings("unchecked") Map<String, SolaceHeaderMeta<?>> solaceHeaderMeta =
				(Map<String, SolaceHeaderMeta<?>>) headersMeta;

		Mockito.doReturn(100).when(xmlMessage).getDeliveryCount();

		for (Map.Entry<String, SolaceHeaderMeta<?>> headerMeta : solaceHeaderMeta.entrySet()) {
			if (!headerMeta.getValue().isReadable()) continue;
			assertThat(headerMeta.getKey(), headerMeta.getValue().getReadAction().apply(xmlMessage),
					anyOf(instanceOf(headerMeta.getValue().getType()), nullValue()));
		}
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testMetaWriteActions(Class<?> headersClass, Map<String, ? extends HeaderMeta<?>> headersMeta)
			throws Exception {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			LOGGER.info("Test does not apply to {}", headersClass.getSimpleName());
			return;
		}

		XMLMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		@SuppressWarnings("unchecked") Map<String, SolaceHeaderMeta<?>> solaceHeaderMeta =
				(Map<String, SolaceHeaderMeta<?>>) headersMeta;

		for (Map.Entry<String, SolaceHeaderMeta<?>> headerMeta : solaceHeaderMeta.entrySet()) {
			if (!headerMeta.getValue().isWritable()) continue;

			Class<?> type = headerMeta.getValue().getType();
			Object value;
			try {
				if (Number.class.isAssignableFrom(type)) {
					value = type.getConstructor(String.class).newInstance("" + RandomUtils.nextInt(0, 100));
				} else if (Boolean.class.isAssignableFrom(type)) {
					value = true;
				} else if (String.class.isAssignableFrom(type)) {
					value = RandomStringUtils.randomAlphanumeric(10);
				} else if (Destination.class.isAssignableFrom(type)) {
					value = JCSMPFactory.onlyInstance().createQueue(RandomStringUtils.randomAlphanumeric(10));
				} else if (byte[].class.isAssignableFrom(type)) {
					value = RandomStringUtils.random(10).getBytes();
				} else {
					value = type.newInstance();
				}
			} catch (Exception e) {
				throw new Exception(String.format("Failed to generate test value for %s", headerMeta.getKey()), e);
			}

			LOGGER.info("Writing {}: {}", headerMeta.getKey(), value);
			headerMeta.getValue().getWriteAction().accept(xmlMessage, value);

			if (headerMeta.getValue().isReadable()) {
				assertEquals(value, headerMeta.getValue().getReadAction().apply(xmlMessage));
			} else {
				LOGGER.warn("No read action for header {}. Cannot validate that write operation worked", headerMeta.getKey());
			}
		}

		LOGGER.info("Message Dump:\n{}", xmlMessage.dump(XMLMessage.MSGDUMP_FULL));
		LOGGER.info("Message String:\n{}", xmlMessage);
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testDefaultValueOverride(Class<?> headersClass, Map<String, ? extends HeaderMeta<?>> headersMeta) {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			LOGGER.info("Test does not apply to {}", headersClass.getSimpleName());
			return;
		}

		XMLMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		@SuppressWarnings("unchecked") Map<String, SolaceHeaderMeta<?>> solaceHeaderMeta =
				(Map<String, SolaceHeaderMeta<?>>) headersMeta;

		for (Map.Entry<String, SolaceHeaderMeta<?>> headerMeta : solaceHeaderMeta.entrySet()) {
			if (!headerMeta.getValue().isWritable()) continue;
			if (!headerMeta.getValue().hasOverriddenDefaultValue()) {
				assertThat(headerMeta.getValue().getDefaultValueOverride(), nullValue());
				continue;
			}

			Object value = headerMeta.getValue().getDefaultValueOverride();

			if (headerMeta.getValue().isReadable()) {
				assertNotEquals(headerMeta.getValue().getReadAction().apply(xmlMessage), value,
						"Overridden default value is the same as the original default value");
			}


			LOGGER.info("Writing {}: {}", headerMeta.getKey(), value);
			headerMeta.getValue().getWriteAction().accept(xmlMessage, value);

			if (headerMeta.getValue().isReadable()) {
				assertEquals(value, headerMeta.getValue().getReadAction().apply(xmlMessage));
			} else {
				LOGGER.warn("No read action for header {}. Cannot validate that write operation worked",
						headerMeta.getKey());
			}
		}

		LOGGER.info("Message Dump:\n{}", xmlMessage.dump(XMLMessage.MSGDUMP_FULL));
		LOGGER.info("Message String:\n{}", xmlMessage);
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testFailMetaWriteActionsWithInvalidType(Class<?> headersClass,
														Map<String, ? extends HeaderMeta<?>> headersMeta) {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			LOGGER.info("Test does not apply to {}", headersClass.getSimpleName());
			return;
		}

		XMLMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		@SuppressWarnings("unchecked") Map<String, SolaceHeaderMeta<?>> solaceHeaderMeta =
				(Map<String, SolaceHeaderMeta<?>>) headersMeta;

		for (Map.Entry<String, SolaceHeaderMeta<?>> headerMeta : solaceHeaderMeta.entrySet()) {
			if (!headerMeta.getValue().isWritable()) continue;
			try {
				headerMeta.getValue().getWriteAction().accept(xmlMessage, new Object());
				fail(String.format("Expected write action to fail for header %s", headerMeta.getKey()));
			} catch (IllegalArgumentException e) {
				assertThat(e.getMessage(), containsString(String.format("Expected type %s, but got %s",
						headerMeta.getValue().getType(), Object.class)));
			}
		}
	}

	@ParameterizedTest(name = "[{index}] {0}")
	@ArgumentsSource(SolaceSpringHeaderArgumentsProvider.class)
	public void testNameJmsCompatibility(Class<?> headersClass, Map<String, ? extends HeaderMeta<?>> headersMeta) {
		for (String headerName : getAllHeaders(headersClass)) {
			assertTrue(Character.isJavaIdentifierStart(headerName.charAt(0)));
			for (int i = 1; i < headerName.length(); i++) {
				assertTrue(Character.isJavaIdentifierPart(headerName.charAt(i)));
			}

			assertNotEquals("NULL", headerName.toUpperCase());
			assertNotEquals("TRUE", headerName.toUpperCase());
			assertNotEquals("FALSE", headerName.toUpperCase());
			assertNotEquals("NOT", headerName.toUpperCase());
			assertNotEquals("AND", headerName.toUpperCase());
			assertNotEquals("OR", headerName.toUpperCase());
			assertNotEquals("BETWEEN", headerName.toUpperCase());
			assertNotEquals("LIKE", headerName.toUpperCase());
			assertNotEquals("IN", headerName.toUpperCase());
			assertNotEquals("IS", headerName.toUpperCase());
			assertNotEquals("ESCAPE", headerName.toUpperCase());

			assertThat(headerName, not(startsWithIgnoringCase("JMSX")));
			assertThat(headerName, not(startsWithIgnoringCase("JMS_")));

			if (!headersClass.equals(SolaceHeaders.class)
					&& HeaderMeta.Scope.WIRE.equals(headersMeta.get(headerName).getScope())) {
				assertThat(headersMeta.get(headerName).getType(),
						anyOf(equalToObject(boolean.class), equalToObject(Boolean.class),
								equalToObject(byte.class), equalToObject(Byte.class),
								equalToObject(short.class), equalToObject(Short.class),
								equalToObject(int.class), equalToObject(Integer.class),
								equalToObject(long.class), equalToObject(Long.class),
								equalToObject(float.class), equalToObject(Float.class),
								equalToObject(double.class), equalToObject(Double.class),
								equalToObject(String.class)
						));
			}
		}
	}

	private Field getPrefixField(Class<?> headersClass) throws NoSuchFieldException {
		return headersClass.getDeclaredField("PREFIX");
	}

	private List<Field> getAllHeaderFields(Class<?> headersClass) {
		return Arrays.stream(headersClass.getDeclaredFields())
				.filter(f -> Modifier.isPublic(f.getModifiers()))
				.filter(f -> Modifier.isStatic(f.getModifiers()))
				.collect(Collectors.toList());
	}

	private List<String> getAllHeaders(Class<?> headersClass) {
		return getAllHeaderFields(headersClass).stream().map(f -> {
					try {
						return (String) f.get(null);
					} catch (IllegalAccessException e) {
						throw new RuntimeException(e);
					}
				})
				.collect(Collectors.toList());
	}

	private String camelCaseToSnakeCase(String camelCase) {
		Matcher camelCaseMatcher = Pattern.compile("(?<=[a-z])[A-Z]").matcher(camelCase);
		StringBuilder buffer = new StringBuilder();
		while (camelCaseMatcher.find()) {
			camelCaseMatcher.appendReplacement(buffer, "_" + camelCaseMatcher.group().toLowerCase());
		}
		camelCaseMatcher.appendTail(buffer);
		return buffer.toString();
	}
}
