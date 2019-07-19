/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb.transaction;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.db.impl.DefaultZeebeDbFactory;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public class ZeebeRocksDbIterationTest {

  private final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
      DefaultZeebeDbFactory.getDefaultFactory(DefaultColumnFamily.class);
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private ZeebeTransactionDb<DefaultColumnFamily> zeebeDb;
  private ColumnFamily<DbCompositeKey<DbLong, DbLong>, DbNil> columnFamily;
  private DbLong firstKey;
  private DbLong secondKey;
  private DbCompositeKey<DbLong, DbLong> compositeKey;

  @Before
  public void setup() throws Exception {
    final File pathName = temporaryFolder.newFolder();
    zeebeDb = Mockito.spy(((ZeebeTransactionDb<DefaultColumnFamily>) dbFactory.createDb(pathName)));

    firstKey = new DbLong();
    secondKey = new DbLong();
    compositeKey = new DbCompositeKey<>(firstKey, secondKey);
    columnFamily =
        zeebeDb.createColumnFamily(
            DefaultColumnFamily.DEFAULT, zeebeDb.createContext(), compositeKey, DbNil.INSTANCE);
  }

  @Test
  public void shouldStopIteratingAfterPrefixExceeded() {
    // given
    final AtomicReference<RocksIterator> spyIterator = new AtomicReference<>();
    Mockito.doAnswer(
            invocation -> {
              final Object spy = Mockito.spy(invocation.callRealMethod());
              spyIterator.set((RocksIterator) spy);
              return spy;
            })
        .when(zeebeDb)
        .newIterator(
            Mockito.anyLong(), Mockito.any(DbContext.class), Mockito.any(ReadOptions.class));

    final long prefixes = 3;
    final long suffixes = 5;

    for (long prefix = 0; prefix < prefixes; prefix++) {
      firstKey.wrapLong(prefix);
      for (long suffix = 0; suffix < suffixes; suffix++) {
        secondKey.wrapLong(suffix);
        columnFamily.put(compositeKey, DbNil.INSTANCE);
      }
    }

    // when
    firstKey.wrapLong(1);
    columnFamily.whileEqualPrefix(firstKey, ((key, value) -> {}));

    // then
    Mockito.verify(spyIterator.get(), Mockito.times((int) suffixes)).next();
  }
}
