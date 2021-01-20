package com.solace.spring.cloud.stream.binder.messaging;

import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLMessage;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.messaging.MessageHeaders;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SolaceHeadersTest {
	@Parameterized.Parameter
	public String parameterSetName; // Only used for parameter set naming

	@Parameterized.Parameter(1)
	public Class<?> headersClass;

	@Parameterized.Parameter(2)
	public Map<String, ? extends HeaderMeta<?>> headersMeta;

	private static final Log logger = LogFactory.getLog(SolaceHeadersTest.class);

	@Parameterized.Parameters(name = "{0}")
	public static Collection<?> headerSets() {
		return Arrays.asList(new Object[][]{
				{SolaceHeaders.class.getSimpleName(), SolaceHeaders.class, SolaceHeaderMeta.META},
				{SolaceBinderHeaders.class.getSimpleName(), SolaceBinderHeaders.class, SolaceBinderHeaderMeta.META}
		});
	}

	@Test
	public void testPrefix() throws Exception {
		Field field = getPrefixField();
		assertEquals(String.format("%s is not a String", field.getName()), String.class, field.getType());
		assertTrue(String.format("%s is not static", field.getName()), Modifier.isStatic(field.getModifiers()));
		assertTrue(String.format("%s is not final", field.getName()), Modifier.isFinal(field.getModifiers()));
		assertThat((String) field.get(null), startsWith(SolaceHeaders.PREFIX));
		assertTrue(((String) field.get(null)).matches("[a-z][a-z_]+_"));

		if (headersClass != SolaceHeaders.class) {
			assertNotEquals(field.get(null), SolaceHeaders.PREFIX);
		}
	}

	@Test
	public void testFieldDeclaration() {
		for (Field field : getAllHeaderFields()) {
			assertEquals(String.format("%s is not a String", field.getName()), String.class, field.getType());
			assertTrue(String.format("%s is not final", field.getName()), Modifier.isFinal(field.getModifiers()));
		}
	}

	@Test
	public void testFieldNameSyntax() throws Exception {
		for (Field field : getAllHeaderFields()) {
			assertTrue(String.format("%s name does not have proper syntax", field.getName()),
					field.getName().matches("[A-Z][A-Z_]+[A-Z]"));

			assertThat(String.format("%s name should not start with prefix", field.getName()),
					field.getName(), not(startsWithIgnoringCase((String) getPrefixField().get(null))));

			String noPrefixHeader = ((String) field.get(null))
					.substring(((String) getPrefixField().get(null)).length());
			assertEquals(String.format(
					"%s name should be the prefix-trimmed, fully-capitalized, '_'-delimited version of %s",
					field.getName(), field.get(null)),
					camelCaseToSnakeCase(noPrefixHeader).toUpperCase(), field.getName());
		}
	}

	@Test
	public void testHeaderSyntax() throws Exception {
		for (String header : getAllHeaders()) {
			String prefix = (String) getPrefixField().get(null);
			assertThat(header, startsWith(prefix));
			assertTrue(String.format("%s does not have proper syntax", header),
					header.matches(prefix + "[a-z][a-zA-Z]+"));
		}
	}

	@Test
	public void testUniqueHeaders() {
		List<String> headers = getAllHeaders();
		assertEquals(String.join(", ", headers) + " does not have unique values",
				headers.stream().distinct().count(), headers.size());
	}

	@Test
	public void testHeadersHaveMetaObjects() {
		List<String> headers = getAllHeaders();
		assertEquals(headersMeta.size(), headers.size());
		for (String header : headers) {
			assertThat(headersMeta.keySet(), hasItem(header));
		}
	}

	@Test
	public void testValidMeta() {
		headersMeta.values()
				.forEach(m -> {
					assertNotNull(m.getType());
					assertFalse(String.format("primitives not supported by %s", MessageHeaders.class.getSimpleName()),
							m.getType().isPrimitive());
				});
	}

	@Test
	public void testUniqueMetaNames() {
		assertEquals(String.join(", ", headersMeta.keySet()) + " does not have unique values",
				headersMeta.keySet().size(), headersMeta.keySet().stream().distinct().count());
	}

	@Test
	public void testMetaReadActions() {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			logger.info(String.format("Test does not apply to %s", headersClass.getSimpleName()));
			return;
		}

		XMLMessage xmlMessage = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
		@SuppressWarnings("unchecked") Map<String, SolaceHeaderMeta<?>> solaceHeaderMeta =
				(Map<String, SolaceHeaderMeta<?>>) headersMeta;

		for (Map.Entry<String, SolaceHeaderMeta<?>> headerMeta : solaceHeaderMeta.entrySet()) {
			if (!headerMeta.getValue().isReadable()) continue;
			assertThat(headerMeta.getKey(), headerMeta.getValue().getReadAction().apply(xmlMessage),
					anyOf(instanceOf(headerMeta.getValue().getType()), nullValue()));
		}
	}

	@Test
	public void testMetaWriteActions() throws Exception {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			logger.info(String.format("Test does not apply to %s", headersClass.getSimpleName()));
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
					value = type.getConstructor(String.class).newInstance("" + RandomUtils.nextInt(100));
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

			logger.info(String.format("Writing %s: %s", headerMeta.getKey(), value));
			headerMeta.getValue().getWriteAction().accept(xmlMessage, value);

			if (headerMeta.getValue().isReadable()) {
				assertEquals(value, headerMeta.getValue().getReadAction().apply(xmlMessage));
			} else {
				logger.warn(String.format("No read action for header %s. Cannot validate that write operation worked",
						headerMeta.getKey()));
			}
		}

		logger.info("Message Dump:\n" + xmlMessage.dump(XMLMessage.MSGDUMP_FULL));
		logger.info("Message String:\n" + xmlMessage.toString());
	}

	@Test
	public void testFailMetaWriteActionsWithInvalidType() {
		if (!(headersClass.equals(SolaceHeaders.class))) {
			logger.info(String.format("Test does not apply to %s", headersClass.getSimpleName()));
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

	@Test
	public void testNameJmsCompatibility() {
		for (String headerName : getAllHeaders()) {
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

			if (!(headersClass.equals(SolaceHeaders.class))) {
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

	private Field getPrefixField() throws NoSuchFieldException {
		return headersClass.getDeclaredField("PREFIX");
	}

	private List<Field> getAllHeaderFields() {
		return Arrays.stream(headersClass.getDeclaredFields())
				.filter(f -> Modifier.isPublic(f.getModifiers()))
				.filter(f -> Modifier.isStatic(f.getModifiers()))
				.collect(Collectors.toList());
	}

	private List<String> getAllHeaders() {
		return getAllHeaderFields().stream().map(f -> {
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
		StringBuffer buffer = new StringBuffer();
		while (camelCaseMatcher.find()) {
			camelCaseMatcher.appendReplacement(buffer, "_" + camelCaseMatcher.group().toLowerCase());
		}
		camelCaseMatcher.appendTail(buffer);
		return buffer.toString();
	}
}
