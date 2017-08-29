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
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

public class Ddl implements Serializable {

  private static final String ROOT = "#>@";
  private static final long serialVersionUID = -4153759448351855360L;

  private ImmutableMap<String, Table> tables;
  private ImmutableMultimap<String, String> parents;

  private Ddl(ImmutableMap<String, Table> tables, ImmutableMultimap<String, String> parents) {
    this.tables = tables;
    this.parents = parents;
  }

  public static Ddl scanInformationSchema(DatabaseClient db) {
    return new InformationSchemaScanner(db).scan();
  }

  public Collection<Table> allTables() {
    return tables.values();
  }

  public Collection<Table> rootTables() {
    return Collections2.transform(parents.get(ROOT), new Function<String, Table>() {

      @Nullable @Override public Table apply(@Nullable String input) {
        return tables.get(input);
      }
    });
  }

  public void prettyPrint(Appendable appendable) throws IOException {
    LinkedList<String> stack = Lists.newLinkedList();
    stack.addAll(parents.get(ROOT));
    boolean first = true;
    Set<String> visited = Sets.newHashSet();
    // Print depth first.
    while (!stack.isEmpty()) {
      if (first) {
        first = false;
      } else {
        appendable.append("\n");
      }
      Table table = tables.get(stack.pollFirst());
      if (visited.contains(table.name())) {
        throw new IllegalStateException("Cycle!");
      }
      visited.add(table.name());
      table.prettyPrint(appendable);
      ImmutableCollection<String> children = parents.get(table.name());
      if (children != null) {
        List<String> tmp = new ArrayList<>(children);
        for (String name : Lists.reverse(tmp)) {
          stack.addFirst(name);
        }
      }
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

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Map<String, Table> tables = Maps.newLinkedHashMap();
    private LinkedHashMultimap<String, String> parents = LinkedHashMultimap.create();

    public Table.Builder createTable(String name) {
      Table table = tables.get(name);
      if (table == null) {
        return Table.builder().name(name).ddlBuilder(this);
      }
      return table.toBuilder().ddlBuilder(this);
    }

    void addTable(Table table) {
      tables.put(table.name(), table);
      String parent = table.interleaveInParent() == null ? ROOT : table.interleaveInParent();
      parents.put(parent, table.name());
    }

    public Ddl build() {
      return new Ddl(ImmutableMap.copyOf(tables), ImmutableMultimap.copyOf(parents));
    }
  }

}
