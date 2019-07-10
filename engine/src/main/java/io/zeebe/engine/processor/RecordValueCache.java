/*
 * Zeebe Workflow Engine
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.engine.processor;

import static io.zeebe.engine.processor.TypedEventRegistry.EVENT_REGISTRY;

import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.util.ReflectUtil;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

public class RecordValueCache {

  private final Map<ValueType, UnifiedRecordValue> eventCache;

  public RecordValueCache() {
    final EnumMap<ValueType, UnifiedRecordValue> cache = new EnumMap<>(ValueType.class);
    EVENT_REGISTRY.forEach((t, c) -> cache.put(t, ReflectUtil.newInstance(c)));

    eventCache = Collections.unmodifiableMap(cache);
  }

  public UnifiedRecordValue readRecordValue(LoggedEvent event, ValueType valueType) {
    final UnifiedRecordValue value = eventCache.get(valueType);
    if (value != null) {
      value.reset();
      event.readValue(value);
    }
    return value;
  }
}
