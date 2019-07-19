/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.msgpack.jsonpath;

import io.zeebe.msgpack.filter.ArrayIndexFilter;
import io.zeebe.msgpack.filter.MapValueWithKeyFilter;
import io.zeebe.msgpack.filter.MsgPackFilter;
import io.zeebe.msgpack.filter.RootCollectionFilter;
import io.zeebe.msgpack.filter.WildcardFilter;
import io.zeebe.msgpack.query.MsgPackFilterContext;
import java.nio.charset.StandardCharsets;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** Can be reused, but is not thread-safe */
public class JsonPathQueryCompiler implements JsonPathTokenVisitor {
  protected static final int ROOT_COLLECTION_FILTER_ID = 0;
  protected static final int MAP_VALUE_FILTER_ID = 1;
  protected static final int ARRAY_INDEX_FILTER_ID = 2;
  protected static final int WILDCARD_FILTER_ID = 3;
  protected static final MsgPackFilter[] JSON_PATH_FILTERS = new MsgPackFilter[4];

  static {
    JSON_PATH_FILTERS[ROOT_COLLECTION_FILTER_ID] = new RootCollectionFilter();
    JSON_PATH_FILTERS[MAP_VALUE_FILTER_ID] = new MapValueWithKeyFilter();
    JSON_PATH_FILTERS[ARRAY_INDEX_FILTER_ID] = new ArrayIndexFilter();
    JSON_PATH_FILTERS[WILDCARD_FILTER_ID] = new WildcardFilter();
  }

  protected JsonPathTokenizer tokenizer = new JsonPathTokenizer();
  protected UnsafeBuffer expressionBuffer = new UnsafeBuffer(0, 0);
  protected ParsingMode mode;
  private JsonPathQuery currentQuery;

  public JsonPathQuery compile(String jsonPathExpression) {
    expressionBuffer.wrap(jsonPathExpression.getBytes(StandardCharsets.UTF_8));
    return compile(expressionBuffer, 0, expressionBuffer.capacity());
  }

  public JsonPathQuery compile(DirectBuffer buffer, int offset, int length) {
    currentQuery = new JsonPathQuery(JSON_PATH_FILTERS);
    currentQuery.wrap(buffer, offset, length);

    // the path starts with the variable name
    mode = ParsingMode.LITERAL;

    // extract algorithm expect a root filter first
    final MsgPackFilterContext filterInstances = currentQuery.getFilterInstances();
    filterInstances.appendElement();
    filterInstances.filterId(ROOT_COLLECTION_FILTER_ID);

    tokenizer.tokenize(buffer, offset, length, this);
    final JsonPathQuery returnValue = currentQuery;
    currentQuery = null;
    return returnValue;
  }

  @Override
  public void visit(
      JsonPathToken type, DirectBuffer valueBuffer, int valueOffset, int valueLength) {
    if (!currentQuery.isValid()) {
      // ignore tokens once query is invalid
      return;
    }

    if (mode == ParsingMode.OPERATOR) {
      switch (type) {
        case CHILD_OPERATOR:
          mode = ParsingMode.LITERAL;
          return;
        case START_INPUT:
        case END_INPUT:
          return; // ignore
        default:
          currentQuery.invalidate(valueOffset, "Unexpected json-path token " + type);
      }

    } else if (mode == ParsingMode.LITERAL) {
      switch (type) {
        case LITERAL:
          final MsgPackFilterContext filterInstances = currentQuery.getFilterInstances();

          // the first literal is the variable name
          if (filterInstances.size() == 1) {
            final byte[] variable = new byte[valueLength];
            valueBuffer.getBytes(valueOffset, variable);
            currentQuery.setVariableName(variable);
          }

          filterInstances.appendElement();
          filterInstances.filterId(MAP_VALUE_FILTER_ID);
          MapValueWithKeyFilter.encodeDynamicContext(
              filterInstances.dynamicContext(), valueBuffer, valueOffset, valueLength);

          mode = ParsingMode.OPERATOR;
          return;
        case START_INPUT:
        case END_INPUT:
          return; // ignore
        default:
          currentQuery.invalidate(valueOffset, "Unexpected json-path token " + type);
      }
    }
  }

  protected enum ParsingMode {
    OPERATOR,
    LITERAL,
  }
}
