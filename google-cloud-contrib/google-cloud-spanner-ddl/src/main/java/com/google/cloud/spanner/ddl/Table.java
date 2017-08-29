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

import com.google.auto.value.AutoValue;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import javax.annotation.Nullable;

@AutoValue
public abstract class Table implements Serializable {

  private static final long serialVersionUID = 1295819360440139056L;

  @Nullable
  public abstract String name();

  @Nullable
  public abstract String interleaveInParent();

  @Nullable
  public abstract ImmutableList<IndexColumn> primaryKey();

  public abstract boolean onDeleteCascade();

  public abstract ImmutableList<Column> columns();

  public abstract ImmutableList<Index> indexes();

  @Nullable
  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_Table.Builder().onDeleteCascade(false);
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE TABLE ").append(name()).append(" (");
    for (Column column : columns()) {
      appendable.append("\n\t");
      column.prettyPrint(appendable);
      appendable.append(",");
    }
    if (primaryKey() != null) {
      appendable.append("\n) PRIMARY KEY (").append(Joiner.on(", ").join(primaryKey()));
    }
    appendable.append(")");
    if (interleaveInParent() != null) {
      appendable.append("\nINTERLEAVE IN PARENT ").append(interleaveInParent());
      if (onDeleteCascade()) {
        appendable.append(" ON DELETE CASCADE");
      }
    }
    for (Index index : indexes()) {
      appendable.append("\n");
      index.prettyPrint(appendable);
    }
  }

  public String prettyPrint()  {
    StringBuilder sb = new StringBuilder();
    try {

      prettyPrint(sb);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return prettyPrint();
  }


  @AutoValue.Builder
  public abstract static class Builder implements IndexColumn.ReturnsCallback {

    private Ddl.Builder ddlBuilder;


    Builder ddlBuilder(Ddl.Builder ddlBuilder) {
      this.ddlBuilder = ddlBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract Builder interleaveInParent(String parent);

    public abstract Builder primaryKey(ImmutableList<IndexColumn> primaryKey);

    abstract Builder onDeleteCascade(boolean onDeleteCascade);

    abstract ImmutableList.Builder<Column> columnsBuilder();

    abstract ImmutableList.Builder<Index> indexesBuilder();

    public IndexColumn.IndexColumnsBuilder<Builder> primaryKey() {
      return new IndexColumn.IndexColumnsBuilder<>(this);
    }

    @Override
    public void addIndexColumns(ImmutableList<IndexColumn> columns) {
      primaryKey(columns);
    }

    public Index.Builder createIndex(String name) {
      return Index.builder().name(name).table(autoBuild().name()).nullFiltered(false).unique(false)
          .tableBuilder
          (this);
    }

    public Column.Builder column(String name) {
      return Column.builder().name(name).tableBuilder(this);
    }

    public Builder onDeleteCascade() {
      onDeleteCascade(true);
      return this;
    }

    abstract Table autoBuild();

    public Ddl.Builder endTable() {
      ddlBuilder.addTable(this.autoBuild());
      return ddlBuilder;
    }
  }
}
