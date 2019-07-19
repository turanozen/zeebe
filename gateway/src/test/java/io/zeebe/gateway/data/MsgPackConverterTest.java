/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.data;

import static io.zeebe.util.StringUtil.getBytes;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.util.LangUtil;
import io.zeebe.util.StreamUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

public class MsgPackConverterTest {

  protected static final String JSON = "{\"key1\":1,\"key2\":2}";
  protected static final byte[] MSG_PACK = createMsgPack();
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void shouldConvertFromJsonStringToMsgPack() {
    // when
    final byte[] msgPack = MsgPackConverter.convertToMsgPack(JSON);

    // then
    assertThat(msgPack).isEqualTo(MSG_PACK);
  }

  @Test
  public void shouldConvertFromJsonStreamToMsgPack() {
    // given
    final byte[] json = getBytes(JSON);
    final InputStream inputStream = new ByteArrayInputStream(json);
    // when
    final byte[] msgPack = MsgPackConverter.convertToMsgPack(inputStream);

    // then
    assertThat(msgPack).isEqualTo(MSG_PACK);
  }

  @Test
  public void shouldConvertFromMsgPackToJsonString() {
    // when
    final String json = MsgPackConverter.convertToJson(MSG_PACK);

    // then
    assertThat(json).isEqualTo(JSON);
  }

  @Test
  public void shouldConvertFromMsgPackToJsonStream() throws Exception {
    // when
    final InputStream jsonStream = MsgPackConverter.convertToJsonInputStream(MSG_PACK);

    // then
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    StreamUtil.copy(jsonStream, outputStream);

    final byte[] jsonBytes = outputStream.toByteArray();

    assertThat(new String(jsonBytes, StandardCharsets.UTF_8)).isEqualTo(JSON);
  }

  @Test
  public void shouldConvertStringFromMsgPackToJsonString() {
    // when
    final String json =
        MsgPackConverter.convertToJson(
            MsgPackUtil.encodeMsgPack(b -> b.packString("x")).byteArray());

    // then
    assertThat(json).isEqualTo("\"x\"");
  }

  @Test
  public void shouldConvertIntegerFromMsgPackToJsonString() {
    // when
    final String json =
        MsgPackConverter.convertToJson(MsgPackUtil.encodeMsgPack(b -> b.packInt(123)).byteArray());

    // then
    assertThat(json).isEqualTo("123");
  }

  @Test
  public void shouldConvertBooleanFromMsgPackToJsonString() {
    // when
    final String json =
        MsgPackConverter.convertToJson(
            MsgPackUtil.encodeMsgPack(b -> b.packBoolean(true)).byteArray());

    // then
    assertThat(json).isEqualTo("true");
  }

  @Test
  public void shouldConvertArrayFromMsgPackToJsonString() {
    // when
    final String json =
        MsgPackConverter.convertToJson(
            MsgPackUtil.encodeMsgPack(b -> b.packArrayHeader(2).packInt(1).packInt(2)).byteArray());

    // then
    assertThat(json).isEqualTo("[1,2]");
  }

  @Test
  public void shouldConvertNullFromMsgPackToJsonString() {
    // when
    final String json =
        MsgPackConverter.convertToJson(
            MsgPackUtil.encodeMsgPack(MessagePacker::packNil).byteArray());

    // then
    assertThat(json).isEqualTo("null");
  }

  @Test
  public void shouldThrowExceptionIfNotAJsonObject() {
    // then
    exception.expect(RuntimeException.class);
    exception.expectMessage("Failed to convert JSON to MessagePack");

    // when
    MsgPackConverter.convertToMsgPack("}");
  }

  @Test
  public void shouldThrowExceptionIfDocumentHasMoreThanOneObject() {
    // then
    exception.expect(RuntimeException.class);
    exception.expectMessage("Failed to convert JSON to MessagePack");

    // when
    MsgPackConverter.convertToMsgPack("{}{}");
  }

  protected static byte[] createMsgPack() {
    byte[] msgPack = null;

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      final MessagePacker variablesPacker = MessagePack.newDefaultPacker(outputStream);

      variablesPacker.packMapHeader(2).packString("key1").packInt(1).packString("key2").packInt(2);

      variablesPacker.flush();
      msgPack = outputStream.toByteArray();
    } catch (Exception e) {
      LangUtil.rethrowUnchecked(e);
    }
    return msgPack;
  }
}
