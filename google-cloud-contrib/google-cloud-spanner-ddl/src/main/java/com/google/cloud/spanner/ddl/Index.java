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
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import javax.annotation.Nullable;

@AutoValue
public abstract class Index implements Serializable {

  private static final long serialVersionUID = 7435575480487550039L;

  abstract String name();

  abstract String table();

  abstract ImmutableList<IndexColumn> indexColumns();

  abstract boolean unique();

  abstract boolean nullFiltered();

  @Nullable abstract String interleaveIn();

  public static Builder builder() {
    return new AutoValue_Index.Builder();
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append("CREATE");
    if (unique()) {
      appendable.append(" UNIQUE");
    }
    if (unique()) {
      appendable.append(" NULL_FILTERED");
    }
    appendable.append(" INDEX ").append(name()).append(" ON ").append(table());

    appendable.append("(");
    for (IndexColumn column : Collections2
        .filter(indexColumns(), Predicates.not(STORING_PREDICATE))) {
      column.prettyPrint(appendable);
    }
    appendable.append(")");

    Collection<String> storing = Collections2
        .transform(Collections2.filter(indexColumns(), STORING_PREDICATE),
            new Function<IndexColumn, String>() {

              @Nullable @Override public String apply(@Nullable IndexColumn input) {
                return input.name();
              }
            });
    String storingString = Joiner.on(", ").join(storing);

    if (!storingString.isEmpty()) {
      appendable.append(" STORING (").append(storingString).append(")");
    }
    if (interleaveIn() != null) {
      appendable.append(" INTERLEAVE IN ").append(interleaveIn());
    }
  }

  public String prettyPrint() {
    StringBuilder sb = new StringBuilder();
    try {
      prettyPrint(sb);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return sb.toString();
  }

  @Override public String toString() {
    return prettyPrint();
  }

  @AutoValue.Builder public abstract static class Builder implements IndexColumn.ReturnsCallback {

    private Table.Builder tableBuilder;

    public Builder tableBuilder(Table.Builder tableBuilder) {
      this.tableBuilder = tableBuilder;
      return this;
    }

    public abstract Builder name(String name);

    public abstract Builder table(String name);

    abstract Builder indexColumns(ImmutableList<IndexColumn> columns);

    public IndexColumn.IndexColumnsBuilder<Builder> columns() {
      return new IndexColumn.IndexColumnsBuilder<>(this);
    }

    @Override
    public void addIndexColumns(ImmutableList<IndexColumn> columns) {
      indexColumns(columns);
    }

    public abstract Builder unique(boolean unique);

    public Builder unique() {
      return unique(true);
    }

    public abstract Builder nullFiltered(boolean nullFiltered);

    public Builder nullFiltered() {
      return nullFiltered(true);
    }

    public abstract Builder interleaveIn(String interleaveIn);

    abstract Index autoBuild();

    public Table.Builder endIndex() {
      tableBuilder.indexesBuilder().add(this.autoBuild());
      return tableBuilder;
    }
  }

  private static final Predicate<IndexColumn> STORING_PREDICATE = new Predicate<IndexColumn>() {

    @Override public boolean apply(@Nullable IndexColumn input) {
      return input.order() == IndexColumn.Order.STORING;
    }
  };
}
