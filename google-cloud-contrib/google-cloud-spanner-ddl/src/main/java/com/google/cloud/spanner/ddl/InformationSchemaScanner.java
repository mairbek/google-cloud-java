/*
 * Copyright 2016 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spanner.ddl;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

class InformationSchemaScanner {
  private final DatabaseClient mydb;

  InformationSchemaScanner(DatabaseClient mydb) {
    this.mydb = mydb;
  }

  Ddl scan() {
    Ddl.Builder builder = Ddl.builder();
    listTables(builder);
    listColumns(builder);

    // TODO(mairbek): indexes and columns
    return builder.build();
  }

  private void listTables(Ddl.Builder builder) {
    ReadContext readContext = mydb.singleUse();
    ResultSet resultSet = readContext.executeQuery(Statement
        .of("SELECT t.table_name, t.parent_table_name FROM information_schema.tables AS t WHERE t"
            + ".table_catalog = '' and" + " t.table_schema = ''"));
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String parentTableName = resultSet.isNull(1) ? null : resultSet.getString(1);
      System.out.println(tableName);
      builder.createTable(tableName).interleaveInParent(parentTableName).endTable();
    }
  }

  private void listColumns(Ddl.Builder builder) {
    ReadContext readContext = mydb.singleUse();
    ResultSet resultSet = readContext.executeQuery(Statement.newBuilder(
        "SELECT c.table_name, c.column_name, c.ordinal_position, c.spanner_type, c.is_nullable from "
            + "information_schema.columns as"
            + " c where c.table_catalog = '' and c.table_schema = '' order by c.table_name, c"
            + ".ordinal_position").build());
    while (resultSet.next()) {
      String tableName = resultSet.getString(0);
      String columnName = resultSet.getString(1);
      String spannerType = resultSet.getString(3);
      String isNullable = resultSet.getString(4);
      boolean nullable = isNullable.toLowerCase().equals("true");
      builder.createTable(tableName).column(columnName).parseType(spannerType).notNull(nullable)
          .endColumn().endTable();
    }
  }
}
