package io.trino.plugin.tidb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SchemaTableName;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.catalog.Catalog;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiTableInfo;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

public class TiDBClient
{
    private final TiSession session;

    @Inject
    public TiDBClient(TiDBConfig config)
    {
        TiConfiguration conf = TiConfiguration.createRawDefault(config.getURL().toString());
        this.session = TiSession.create(conf);
    }

    public List<String> listSchemaNames()
    {
        return session.getCatalog().listDatabases().stream().map(db -> db.getName()).toList();
    }

    public List<SchemaTableName> listTables(String schemaName)
    {
        Catalog catalog = session.getCatalog();
        TiDBInfo db = catalog.getDatabase(schemaName);
        return session.getCatalog().listTables(db).stream()
                .map(tableInfo -> new SchemaTableName(schemaName, tableInfo.getName()))
                .toList();
    }

    public TiTableInfo getTable(String schemaName, String tableName) {
        return session.getCatalog().getTable(schemaName, tableName);
    }
}
