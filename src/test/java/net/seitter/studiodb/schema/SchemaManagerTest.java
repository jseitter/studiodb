package net.seitter.studiodb.schema;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.IBufferPoolManager;

/**
 * Tests for the SchemaManager class.
 */
public class SchemaManagerTest {
    
    @TempDir
    File tempDir;
    
    private DatabaseSystem dbSystem;
    private SchemaManager schemaManager;
    private String testTablespacePath;
    
    @BeforeEach
    public void setUp() {
        // Set up paths
        testTablespacePath = new File(tempDir, "test_tablespace.dat").getAbsolutePath();
        
        // Create a fresh DatabaseSystem for each test
        System.setProperty("studiodb.data.dir", tempDir.getAbsolutePath());
        dbSystem = new DatabaseSystem();
        schemaManager = dbSystem.getSchemaManager();
        
        // Create a test tablespace
        dbSystem.createTablespace("TEST_TS", testTablespacePath, 20);
    }
    
    @AfterEach
    public void tearDown() {
        if (dbSystem != null) {
            dbSystem.shutdown();
        }
    }
    
    @Test
    public void testCreateTable() {
        // Create column definitions
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("ID", DataType.INTEGER, false));
        columns.add(new Column("NAME", DataType.VARCHAR, false, 50));
        columns.add(new Column("AGE", DataType.INTEGER, true));
        
        // Define primary key
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("ID");
        
        // Create the table
        Table table = schemaManager.createTable("PERSON", "TEST_TS", columns, pkColumns);
        
        // Verify table was created
        assertNotNull(table, "Table should be created successfully");
        assertEquals("PERSON", table.getName(), "Table name should match");
        assertEquals("TEST_TS", table.getTablespaceName(), "Tablespace name should match");
        
        // Verify columns
        List<Column> tableColumns = table.getColumns();
        assertEquals(3, tableColumns.size(), "Should have 3 columns");
        
        // Verify first column
        Column idColumn = tableColumns.get(0);
        assertEquals("ID", idColumn.getName(), "First column name should be ID");
        assertEquals(DataType.INTEGER, idColumn.getDataType(), "First column should be INTEGER");
        assertFalse(idColumn.isNullable(), "ID column should not be nullable");
        
        // Verify primary key
        List<String> primaryKey = table.getPrimaryKey();
        assertEquals(1, primaryKey.size(), "Should have 1 primary key column");
        assertEquals("ID", primaryKey.get(0), "Primary key should be ID");
        
        // Verify the table is in schema manager's cache
        Table retrievedTable = schemaManager.getTable("PERSON");
        assertNotNull(retrievedTable, "Should be able to retrieve the table");
        assertEquals(table.getHeaderPageId(), retrievedTable.getHeaderPageId(), 
                "Retrieved table should have same header page ID");
    }
    
    @Test
    public void testCreateIndex() {
        // First create a table
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("ID", DataType.INTEGER, false));
        columns.add(new Column("NAME", DataType.VARCHAR, false, 50));
        
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("ID");
        
        Table table = schemaManager.createTable("EMPLOYEE", "TEST_TS", columns, pkColumns);
        assertNotNull(table, "Table should be created successfully");
        
        // Now create an index on the NAME column
        List<String> indexColumns = new ArrayList<>();
        indexColumns.add("NAME");
        
        Index index = schemaManager.createIndex("IDX_EMP_NAME", "EMPLOYEE", indexColumns, true);
        
        // Verify index was created
        assertNotNull(index, "Index should be created successfully");
        assertEquals("IDX_EMP_NAME", index.getName(), "Index name should match");
        assertEquals("EMPLOYEE", index.getTableName(), "Table name should match");
        assertEquals("TEST_TS", index.getTablespaceName(), "Tablespace name should match");
        assertTrue(index.isUnique(), "Index should be unique");
        
        // Verify index columns
        List<String> actualIndexColumns = index.getColumnNames();
        assertEquals(1, actualIndexColumns.size(), "Should have 1 indexed column");
        assertEquals("NAME", actualIndexColumns.get(0), "Indexed column should be NAME");
        
        // Verify the index is in schema manager's cache
        Index retrievedIndex = schemaManager.getIndex("IDX_EMP_NAME");
        assertNotNull(retrievedIndex, "Should be able to retrieve the index");
        
        // Verify the index is associated with the table
        List<Index> tableIndexes = schemaManager.getIndexesForTable("EMPLOYEE");
        assertEquals(1, tableIndexes.size(), "Should have 1 index on the table");
        assertEquals("IDX_EMP_NAME", tableIndexes.get(0).getName(), "Index name should match");
    }
    
    @Test
    public void testDropTable() {
        // First create a table
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("ID", DataType.INTEGER, false));
        columns.add(new Column("VALUE", DataType.VARCHAR, true, 100));
        
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("ID");
        
        Table table = schemaManager.createTable("TEST_TABLE", "TEST_TS", columns, pkColumns);
        assertNotNull(table, "Table should be created successfully");
        
        // Create an index on the table
        List<String> indexColumns = new ArrayList<>();
        indexColumns.add("VALUE");
        
        Index index = schemaManager.createIndex("IDX_TEST_VALUE", "TEST_TABLE", indexColumns, false);
        assertNotNull(index, "Index should be created successfully");
        
        // Now drop the table
        boolean dropped = schemaManager.dropTable("TEST_TABLE");
        assertTrue(dropped, "Table should be dropped successfully");
        
        // Verify the table is no longer in the schema manager
        Table droppedTable = schemaManager.getTable("TEST_TABLE");
        assertNull(droppedTable, "Table should no longer exist");
        
        // Verify the index is also removed
        Index droppedIndex = schemaManager.getIndex("IDX_TEST_VALUE");
        assertNull(droppedIndex, "Index should also be dropped");
        
        // Verify the indexes for table list is empty
        List<Index> tableIndexes = schemaManager.getIndexesForTable("TEST_TABLE");
        assertTrue(tableIndexes.isEmpty(), "Should have no indexes for dropped table");
    }
    
    @Test
    public void testSystemTablesExist() {
        // Verify system catalog tables exist
        assertNotNull(schemaManager.getTable("SYS_TABLESPACES"), "SYS_TABLESPACES table should exist");
        assertNotNull(schemaManager.getTable("SYS_TABLES"), "SYS_TABLES table should exist");
        assertNotNull(schemaManager.getTable("SYS_COLUMNS"), "SYS_COLUMNS table should exist");
        assertNotNull(schemaManager.getTable("SYS_INDEXES"), "SYS_INDEXES table should exist");
        assertNotNull(schemaManager.getTable("SYS_INDEX_COLUMNS"), "SYS_INDEX_COLUMNS table should exist");
        
        // Check structure of SYS_TABLESPACES
        Table tablespaceTable = schemaManager.getTable("SYS_TABLESPACES");
        List<Column> columns = tablespaceTable.getColumns();
        assertTrue(columns.size() >= 3, "SYS_TABLESPACES should have at least 3 columns");
        
        // Check that our test tablespace exists in the SYSTEM tablespace
        IBufferPoolManager sysBpm = dbSystem.getBufferPoolManager("SYSTEM");
        assertNotNull(sysBpm, "System buffer pool should exist");
    }
} 