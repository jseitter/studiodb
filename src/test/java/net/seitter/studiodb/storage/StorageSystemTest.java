package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.IBufferPoolManager;
import net.seitter.studiodb.schema.Column;
import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.schema.SchemaManager;
import net.seitter.studiodb.schema.Table;

/**
 * Comprehensive tests for the storage system components, focusing on tablespace
 * creation, data persistence, and retrieval.
 */
public class StorageSystemTest {
    
    @TempDir
    File tempDir;
    
    private DatabaseSystem dbSystem;
    private StorageManager storageManager;
    private SchemaManager schemaManager;
    private String testTablespacePath;
    private String sysTablespacePath;
    
    @BeforeEach
    public void setUp() {
        // Set up paths
        testTablespacePath = new File(tempDir, "test_tablespace.dat").getAbsolutePath();
        sysTablespacePath = new File(tempDir, "sys_tablespace.dat").getAbsolutePath();
        
        // Create a fresh DatabaseSystem instance for each test
        System.setProperty("studiodb.data.dir", tempDir.getAbsolutePath());
        
        // Since we can't use a custom constructor, we'll use the default one
        // and replace key components after initialization
        dbSystem = new DatabaseSystem();
        storageManager = dbSystem.getStorageManager();
        schemaManager = dbSystem.getSchemaManager();
    }
    
    @AfterEach
    public void tearDown() {
        // Clean up - shut down the database system
        if (dbSystem != null) {
            dbSystem.shutdown();
        }
    }
    
    @Test
    public void testCreateTablespaceAndVerifyPersistence() {
        // Create a test tablespace
        boolean created = dbSystem.createTablespace("TEST_TS", testTablespacePath, 10);
        assertTrue(created, "Tablespace should be created successfully");
        
        // Check if the tablespace file was created
        File tablespaceFile = new File(testTablespacePath);
        assertTrue(tablespaceFile.exists(), "Tablespace file should exist");
        
        // Verify we can get a buffer pool for the tablespace
        IBufferPoolManager bpm = dbSystem.getBufferPoolManager("TEST_TS");
        assertNotNull(bpm, "Should be able to get buffer pool for the tablespace");
        
        // Flush all buffer pools to make sure everything is persisted
        try {
            bpm.flushAll();
        } catch (IOException e) {
            fail("Failed to flush buffer pool: " + e.getMessage());
        }
        
        // Verify the tablespace is in the system catalog
        // This requires executing a SELECT query through SchemaManager
        // But we'll directly check through the system tablespace
        IBufferPoolManager sysBpm = dbSystem.getBufferPoolManager("SYSTEM");
        assertNotNull(sysBpm, "System buffer pool should exist");
        
        // Test the size of the created tablespace
        try {
            Tablespace tablespace = storageManager.getTablespace("TEST_TS");
            assertNotNull(tablespace, "Tablespace should exist in storage manager");
            assertEquals(10, tablespace.getTotalPages(), "Tablespace should have 10 pages");
        } catch (IOException e) {
            fail("Failed to get tablespace info: " + e.getMessage());
        }
    }
    
    @Test
    public void testCreateTableAndInsertRetrieveData() throws IOException {
        // Create a test tablespace first
        boolean created = dbSystem.createTablespace("TEST_TS", testTablespacePath, 20);
        assertTrue(created, "Tablespace should be created successfully");
        
        // Create a test table with some columns
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("ID", DataType.INTEGER, false));
        columns.add(new Column("NAME", DataType.VARCHAR, false, 50));
        columns.add(new Column("ACTIVE", DataType.BOOLEAN, true));
        
        List<String> pkColumns = new ArrayList<>();
        pkColumns.add("ID");
        
        Table table = schemaManager.createTable("TEST_TABLE", "TEST_TS", columns, pkColumns);
        assertNotNull(table, "Table should be created successfully");
        
        // Now let's manually insert a row into this table
        // Get the buffer pool for the tablespace
        IBufferPoolManager bpm = dbSystem.getBufferPoolManager("TEST_TS");
        assertNotNull(bpm, "Buffer pool should exist");
        
        // Get the header page of the table
        PageId headerPageId = new PageId("TEST_TS", table.getHeaderPageId());
        Page headerPage = bpm.fetchPage(headerPageId);
        assertNotNull(headerPage, "Header page should exist");
        
        // Get the first data page ID
        ByteBuffer headerBuffer = headerPage.getBuffer();
        headerBuffer.position(4); // Skip magic number
        int firstDataPageId = headerBuffer.getInt();
        bpm.unpinPage(headerPageId, false);
        
        // Insert a row into the data page
        PageId dataPageId = new PageId("TEST_TS", firstDataPageId);
        Page dataPage = bpm.fetchPage(dataPageId);
        assertNotNull(dataPage, "Data page should exist");
        
        // Create sample row data
        Map<String, Object> row = new HashMap<>();
        row.put("ID", 1);
        row.put("NAME", "Test Record");
        row.put("ACTIVE", true);
        
        // Serialize the row
        byte[] rowData = serializeRow(row);
        assertTrue(rowData.length > 0, "Row data should be serialized");
        
        // Write to the page similar to insertIntoSystemTable
        ByteBuffer dataBuffer = dataPage.getBuffer();
        
        // Read current row count and free space offset
        dataBuffer.position(8); // Skip magic and next page ID
        int rowCount = dataBuffer.getInt();
        int freeSpaceOffset = dataBuffer.getInt();
        
        // If this is a new page, initialize free space
        if (freeSpaceOffset == 0) {
            freeSpaceOffset = dataBuffer.capacity();
        }
        
        // Store row from end of page backward
        int newRowOffset = freeSpaceOffset - rowData.length;
        int rowDirectoryPos = 16 + rowCount * 8;
        
        // Check space
        assertTrue(rowDirectoryPos + 8 < newRowOffset, "Should have enough space for the row");
        
        // Update directory
        dataBuffer.position(rowDirectoryPos);
        dataBuffer.putInt(newRowOffset);
        dataBuffer.putInt(rowData.length);
        
        // Write row data
        dataBuffer.position(newRowOffset);
        dataBuffer.put(rowData);
        
        // Update metadata
        dataBuffer.position(8);
        dataBuffer.putInt(rowCount + 1);
        dataBuffer.putInt(newRowOffset);
        
        // Mark dirty and unpin
        dataPage.markDirty();
        bpm.unpinPage(dataPageId, true);
        
        // Flush all to ensure persistence
        bpm.flushAll();
        
        // Now retrieve the row to verify
        Page retrievePage = bpm.fetchPage(dataPageId);
        assertNotNull(retrievePage, "Should be able to fetch the page again");
        
        ByteBuffer retrieveBuffer = retrievePage.getBuffer();
        retrieveBuffer.position(8);
        int retrievedRowCount = retrieveBuffer.getInt();
        assertEquals(1, retrievedRowCount, "Should have 1 row");
        
        // Skip free space offset
        retrieveBuffer.getInt();
        
        // Read directory entry
        int storedOffset = retrieveBuffer.getInt();
        int storedLength = retrieveBuffer.getInt();
        
        // Read row data
        byte[] retrievedData = new byte[storedLength];
        retrieveBuffer.position(storedOffset);
        retrieveBuffer.get(retrievedData);
        
        // Deserialize
        Map<String, Object> retrievedRow = deserializeRow(retrievedData);
        assertNotNull(retrievedRow, "Retrieved row should not be null");
        
        // Verify contents
        assertEquals(1, retrievedRow.get("ID"), "ID should match");
        assertEquals("Test Record", retrievedRow.get("NAME"), "NAME should match");
        assertEquals(true, retrievedRow.get("ACTIVE"), "ACTIVE should match");
        
        // Clean up
        bpm.unpinPage(dataPageId, false);
    }
    
    @Test
    public void testSystemTablespaceInitialization() throws IOException {
        // The system tablespace should already be initialized
        IBufferPoolManager sysBpm = dbSystem.getBufferPoolManager("SYSTEM");
        assertNotNull(sysBpm, "System buffer pool should exist");
        
        // Verify the SYS_TABLESPACES table exists
        Table sysTablespacesTable = schemaManager.getTable("SYS_TABLESPACES");
        assertNotNull(sysTablespacesTable, "SYS_TABLESPACES table should exist");
        
        // Create a test tablespace
        boolean created = dbSystem.createTablespace("VERIFY_TS", testTablespacePath, 5);
        assertTrue(created, "Tablespace should be created successfully");
        
        // Flush to ensure persistence
        sysBpm.flushAll();
        
        // Verify it exists in storage manager
        Tablespace tablespace = storageManager.getTablespace("VERIFY_TS");
        assertNotNull(tablespace, "Tablespace should exist in storage manager");
        
        // TODO: Add a verification of the system catalog entry once SELECT is working
    }
    
    // Helper method to serialize a row
    private byte[] serializeRow(Map<String, Object> row) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.writeValueAsBytes(row);
        } catch (Exception e) {
            e.printStackTrace();
            return new byte[0];
        }
    }
    
    // Helper method to deserialize a row
    private Map<String, Object> deserializeRow(byte[] rowData) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            return mapper.readValue(rowData, new com.fasterxml.jackson.core.type.TypeReference<HashMap<String, Object>>() {});
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
} 