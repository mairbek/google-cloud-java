package com.google.cloud.spanner.ddl;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class PrettyPrintTest {

  @Test
  public void testCanonical() throws Exception {

    String expected = "CREATE TABLE Singers (\n"
        + "\tSingerId                                INT64 NOT NULL,\n"
        + "\tFirstName                               STRING(1024),\n"
        + "\tLastName                                STRING(1024),\n"
        + "\tSingerInfo                              BYTES(MAX),\n"
        + ") PRIMARY KEY (SingerId ASC)\n" + "CREATE TABLE Albums (\n"
        + "\tSingerId                                INT64 NOT NULL,\n"
        + "\tAlbumId                                 INT64 NOT NULL,\n"
        + "\tAlbumTitle                              STRING(1024),\n"
        + ") PRIMARY KEY (SingerId ASC, AlbumId DESC)\n"
        + "INTERLEAVE IN PARENT Singers ON DELETE CASCADE\n"
        + "CREATE UNIQUE NULL_FILTERED INDEX AlbumsByAlbumTitle ON Albums(AlbumTitle DESC) "
        + "STORING (MarketingBudget)";

    Ddl ddl = Ddl.builder()
        .createTable("Singers")
        .column("SingerId").int64().notNull().endColumn()
        .column("FirstName").string().size(1024).endColumn()
        .column("LastName").string().size(1024).endColumn()
        .column("SingerInfo").bytes().max().endColumn()
        .primaryKey().asc("SingerId").end()
        .endTable()
        .createTable("Albums")
        .column("SingerId").int64().notNull().endColumn()
        .column("AlbumId").int64().notNull().endColumn()
        .column("AlbumTitle").string().size(1024).endColumn()
        .primaryKey().asc("SingerId").desc("AlbumId").end()
        .interleaveInParent("Singers").onDeleteCascade()
        .createIndex("AlbumsByAlbumTitle")
        .unique()
        .columns()
        .desc("AlbumTitle")
        .storing("MarketingBudget")
        .end()
        .endIndex()
        .endTable()
        .build();

    assertEquals(expected, ddl.prettyPrint());
  }
}
