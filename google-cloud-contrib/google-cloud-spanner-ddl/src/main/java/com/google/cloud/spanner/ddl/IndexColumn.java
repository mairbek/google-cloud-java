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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;

@AutoValue
public abstract class IndexColumn implements Serializable {

  private static final long serialVersionUID = -976796114694704550L;

  public abstract String name();

  public abstract Order order();

  public static IndexColumn create(String name, Order order) {
    return new AutoValue_IndexColumn(name, order);
  }

  public enum Order {
    ASC("ASC"), DESC("DESC"), STORING("STORING");

    Order(String title) {
      this.title = title;
    }

    public static Order defaultOrder() {
      return ASC;
    }

    private final String title;
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    appendable.append(name()).append(" ").append(order().title);
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

  @Override public String toString() {
    return prettyPrint();
  }


  interface ReturnsCallback {
    void addIndexColumns(ImmutableList<IndexColumn> columns);
  }

  public static class IndexColumnsBuilder<R extends ReturnsCallback> {
    private ImmutableList.Builder<IndexColumn> columns = ImmutableList.builder();

    private R callback;

    public IndexColumnsBuilder(R callback) {
      this.callback = callback;
    }

    public IndexColumnsBuilder<R> asc(String name) {
      columns.add(IndexColumn.create(name, Order.ASC));
      return this;
    }

    public IndexColumnsBuilder<R> desc(String name) {
      columns.add(IndexColumn.create(name, Order.DESC));
      return this;
    }

    public IndexColumnsBuilder<R> storing(String name) {
      columns.add(IndexColumn.create(name, Order.STORING));
      return this;
    }

    public R end() {
      callback.addIndexColumns(columns.build());
      return callback;
    }
  }

}
