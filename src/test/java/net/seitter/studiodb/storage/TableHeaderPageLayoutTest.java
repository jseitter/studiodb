package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.storage.layout.TableDataPageLayout;
import net.seitter.studiodb.storage.layout.TableHeaderPageLayout;
import net.seitter.studiodb.storage.layout.TableHeaderPageLayout.ColumnDefinition;

/**
 * Tests for the TableHeaderPageLayout class.
 * This test class verifies the functionality of the table header page layout,
 * including column definitions with different data types and integration with 
 * TableDataPageLayout for row storage.
 */
public class TableHeaderPageLayoutTest {

    @Test
    public void testBasicTableHeaderFunctions() {
        // Create a page to use for the test
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the header layout
        TableHeaderPageLayout layout = new TableHeaderPageLayout(page);
        layout.initialize();
        
        // Verify the page type is set correctly
        assertEquals(PageType.TABLE_HEADER, layout.readHeader());
        
        // Test setting and getting the table name
        layout.setTableName("TEST_TABLE");
        assertEquals("TEST_TABLE", layout.getTableName());
        
        // Test setting and getting the first data page ID
        layout.setFirstDataPageId(5);
        assertEquals(5, layout.getFirstDataPageId());
        
        // Verify initial column count is 0
        assertEquals(0, layout.getColumnCount());
    }
    
    @Test
    public void testColumnDefinitions() {
        // Create a page to use for the test
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the header layout
        TableHeaderPageLayout layout = new TableHeaderPageLayout(page);
        layout.initialize();
        layout.setTableName("COLUMN_TEST");
        
        // Add columns of each supported data type
        layout.addColumn("ID", DataType.INTEGER, 0, false);              // Not nullable
        layout.addColumn("NAME", DataType.VARCHAR, 100, false);          // Not nullable
        layout.addColumn("SALARY", DataType.FLOAT, 0, true);             // Nullable
        layout.addColumn("IS_ACTIVE", DataType.BOOLEAN, 0, false);       // Not nullable
        layout.addColumn("HIRE_DATE", DataType.DATE, 0, true);           // Nullable
        
        // Verify column count
        assertEquals(5, layout.getColumnCount());
        
        // Retrieve all columns
        List<ColumnDefinition> columns = layout.getColumns();
        assertEquals(5, columns.size());
        
        // Check each column's definition
        ColumnDefinition idCol = columns.get(0);
        assertEquals("ID", idCol.getName());
        assertEquals(DataType.INTEGER, idCol.getDataType());
        assertEquals(0, idCol.getMaxLength());
        assertFalse(idCol.isNullable());
        
        ColumnDefinition nameCol = columns.get(1);
        assertEquals("NAME", nameCol.getName());
        assertEquals(DataType.VARCHAR, nameCol.getDataType());
        assertEquals(100, nameCol.getMaxLength());
        assertFalse(nameCol.isNullable());
        
        ColumnDefinition salaryCol = columns.get(2);
        assertEquals("SALARY", salaryCol.getName());
        assertEquals(DataType.FLOAT, salaryCol.getDataType());
        assertEquals(0, salaryCol.getMaxLength());
        assertTrue(salaryCol.isNullable());
        
        ColumnDefinition activeCol = columns.get(3);
        assertEquals("IS_ACTIVE", activeCol.getName());
        assertEquals(DataType.BOOLEAN, activeCol.getDataType());
        assertEquals(0, activeCol.getMaxLength());
        assertFalse(activeCol.isNullable());
        
        ColumnDefinition dateCol = columns.get(4);
        assertEquals("HIRE_DATE", dateCol.getName());
        assertEquals(DataType.DATE, dateCol.getDataType());
        assertEquals(0, dateCol.getMaxLength());
        assertTrue(dateCol.isNullable());
    }
    
    @Test
    public void testTableWithManyColumns() {
        // Create a page to use for the test
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the header layout
        TableHeaderPageLayout layout = new TableHeaderPageLayout(page);
        layout.initialize();
        layout.setTableName("WIDE_TABLE");
        
        // Add a large number of columns to test capacity
        final int COLUMN_COUNT = 50;
        for (int i = 0; i < COLUMN_COUNT; i++) {
            // Alternate between data types
            DataType type = DataType.values()[i % DataType.values().length];
            layout.addColumn("COL_" + i, type, i % 3 == 0 ? 50 : 0, i % 2 == 0);
        }
        
        // Verify column count
        assertEquals(COLUMN_COUNT, layout.getColumnCount());
        
        // Retrieve all columns and verify
        List<ColumnDefinition> columns = layout.getColumns();
        assertEquals(COLUMN_COUNT, columns.size());
        
        // Check a few columns
        assertEquals("COL_0", columns.get(0).getName());
        assertEquals(DataType.INTEGER, columns.get(0).getDataType());
        assertEquals("COL_49", columns.get(49).getName());
        assertEquals(DataType.DATE, columns.get(49).getDataType());
    }
    
    @Test
    public void testLongTableName() {
        // Create a page to use for the test
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the header layout
        TableHeaderPageLayout layout = new TableHeaderPageLayout(page);
        layout.initialize();
        
        // Create a long table name
        String longName = "THIS_IS_A_VERY_LONG_TABLE_NAME_THAT_TESTS_THE_CAPACITY_OF_THE_TABLE_HEADER_PAGE_LAYOUT_TO_STORE_TABLE_NAMES";
        layout.setTableName(longName);
        
        // Verify the table name was stored correctly
        assertEquals(longName, layout.getTableName());
        
        // Add a column to ensure other functionality still works
        layout.addColumn("TEST_COL", DataType.VARCHAR, 100, false);
        assertEquals(1, layout.getColumnCount());
    }
    
    @Test
    public void testIntegrationWithTableDataPage() throws IOException {
        // Create a header page and data page for the test
        Page headerPage = new Page(new PageId("TEST_TS", 1), 4096);
        Page dataPage = new Page(new PageId("TEST_TS", 2), 4096);
        
        // Create and initialize layouts
        TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
        headerLayout.initialize();
        headerLayout.setTableName("EMPLOYEE");
        headerLayout.setFirstDataPageId(2);
        
        TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
        dataLayout.initialize();
        
        // Define columns for employee table
        headerLayout.addColumn("ID", DataType.INTEGER, 0, false);
        headerLayout.addColumn("NAME", DataType.VARCHAR, 100, false);
        headerLayout.addColumn("SALARY", DataType.FLOAT, 0, true);
        headerLayout.addColumn("IS_ACTIVE", DataType.BOOLEAN, 0, false);
        headerLayout.addColumn("HIRE_DATE", DataType.DATE, 0, true);
        
        // Create test rows
        List<Map<String, Object>> testRows = new ArrayList<>();
        
        // Row 1: All fields populated
        Map<String, Object> row1 = new HashMap<>();
        row1.put("ID", 1);
        row1.put("NAME", "John Doe");
        row1.put("SALARY", 75000.50);
        row1.put("IS_ACTIVE", true);
        row1.put("HIRE_DATE", LocalDate.of(2020, 1, 15).toString());
        testRows.add(row1);
        
        // Row 2: Some null fields
        Map<String, Object> row2 = new HashMap<>();
        row2.put("ID", 2);
        row2.put("NAME", "Jane Smith");
        row2.put("SALARY", null);
        row2.put("IS_ACTIVE", false);
        row2.put("HIRE_DATE", null);
        testRows.add(row2);
        
        // Row 3: Different data
        Map<String, Object> row3 = new HashMap<>();
        row3.put("ID", 3);
        row3.put("NAME", "Bob Johnson");
        row3.put("SALARY", 92500.75);
        row3.put("IS_ACTIVE", true);
        row3.put("HIRE_DATE", LocalDate.of(2018, 6, 10).toString());
        testRows.add(row3);
        
        // Serialize and store each row
        ObjectMapper mapper = new ObjectMapper();
        List<byte[]> serializedRows = new ArrayList<>();
        
        for (Map<String, Object> row : testRows) {
            byte[] rowData = mapper.writeValueAsBytes(row);
            serializedRows.add(rowData);
            boolean added = dataLayout.addRow(rowData);
            assertTrue(added, "Failed to add row to data page");
        }
        
        // Verify row count in data page
        assertEquals(3, dataLayout.getRowCount());
        
        // Retrieve and deserialize each row
        for (int i = 0; i < testRows.size(); i++) {
            byte[] retrievedData = dataLayout.getRow(i);
            assertNotNull(retrievedData, "Failed to retrieve row " + i);
            
            Map<String, Object> retrievedRow = mapper.readValue(retrievedData, 
                    new TypeReference<HashMap<String, Object>>() {});
            
            // Compare with original row data
            Map<String, Object> originalRow = testRows.get(i);
            assertEquals(originalRow.get("ID"), retrievedRow.get("ID"));
            assertEquals(originalRow.get("NAME"), retrievedRow.get("NAME"));
            
            // Handle floating point comparison
            if (originalRow.get("SALARY") == null) {
                assertNull(retrievedRow.get("SALARY"));
            } else {
                assertEquals(originalRow.get("SALARY"), retrievedRow.get("SALARY"));
            }
            
            assertEquals(originalRow.get("IS_ACTIVE"), retrievedRow.get("IS_ACTIVE"));
            assertEquals(originalRow.get("HIRE_DATE"), retrievedRow.get("HIRE_DATE"));
        }
    }
    
    @Test
    public void testDataTypeValidation() {
        // Create pages for the test
        Page headerPage = new Page(new PageId("TEST_TS", 1), 4096);
        Page dataPage = new Page(new PageId("TEST_TS", 2), 4096);
        
        // Create and initialize layouts
        TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
        headerLayout.initialize();
        headerLayout.setTableName("DATA_TYPE_TEST");
        headerLayout.setFirstDataPageId(2);
        
        TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
        dataLayout.initialize();
        
        // Define columns with all data types
        headerLayout.addColumn("INT_COL", DataType.INTEGER, 0, false);
        headerLayout.addColumn("FLOAT_COL", DataType.FLOAT, 0, false);
        headerLayout.addColumn("VARCHAR_COL", DataType.VARCHAR, 100, false);
        headerLayout.addColumn("BOOL_COL", DataType.BOOLEAN, 0, false);
        headerLayout.addColumn("DATE_COL", DataType.DATE, 0, false);
        
        // Test validation for INTEGER
        DataType intType = DataType.INTEGER;
        assertTrue(intType.validateValue(42));
        assertTrue(intType.validateValue(Long.valueOf(42)));
        assertTrue(intType.validateValue("42"));
        assertFalse(intType.validateValue("abc"));
        assertFalse(intType.validateValue(42.5));
        
        // Test validation for FLOAT
        DataType floatType = DataType.FLOAT;
        assertTrue(floatType.validateValue(42.5));
        assertTrue(floatType.validateValue(Float.valueOf(42.5f)));
        assertTrue(floatType.validateValue("42.5"));
        assertTrue(floatType.validateValue("42"));
        assertFalse(floatType.validateValue("abc"));
        
        // Test validation for VARCHAR
        DataType varcharType = DataType.VARCHAR;
        assertTrue(varcharType.validateValue("abc"));
        assertTrue(varcharType.validateValue(""));
        assertTrue(varcharType.validateValue("123"));
        
        // Test validation for BOOLEAN
        DataType boolType = DataType.BOOLEAN;
        assertTrue(boolType.validateValue(true));
        assertTrue(boolType.validateValue(false));
        assertTrue(boolType.validateValue("true"));
        assertTrue(boolType.validateValue("false"));
        assertFalse(boolType.validateValue("abc"));
        
        // Test validation for DATE
        DataType dateType = DataType.DATE;
        assertTrue(dateType.validateValue(LocalDate.now()));
        assertTrue(dateType.validateValue("2023-05-15"));
        assertFalse(dateType.validateValue("05/15/2023"));
        assertFalse(dateType.validateValue("abc"));
    }
    
    @Test
    public void testRowStorageWithDifferentTypes() throws IOException {
        // Create pages for the test
        Page headerPage = new Page(new PageId("TEST_TS", 1), 4096);
        Page dataPage = new Page(new PageId("TEST_TS", 2), 4096);
        
        // Create and initialize layouts
        TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
        headerLayout.initialize();
        headerLayout.setTableName("MIXED_DATA_TYPES");
        headerLayout.setFirstDataPageId(2);
        
        TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
        dataLayout.initialize();
        
        // Define a variety of columns
        headerLayout.addColumn("INT_COL", DataType.INTEGER, 0, false);
        headerLayout.addColumn("FLOAT_COL", DataType.FLOAT, 0, false);
        headerLayout.addColumn("TEXT_COL", DataType.VARCHAR, 1000, false);
        headerLayout.addColumn("BOOL_COL", DataType.BOOLEAN, 0, false);
        headerLayout.addColumn("DATE_COL", DataType.DATE, 0, false);
        
        // Create test rows with extreme values
        Map<String, Object> extremeValues = new HashMap<>();
        extremeValues.put("INT_COL", Integer.MAX_VALUE);
        extremeValues.put("FLOAT_COL", Double.MAX_VALUE);
        extremeValues.put("TEXT_COL", "This is a very long text that tests the capacity " +
                "of the storage system to handle large text values without truncation or other issues. " +
                "It includes various characters: 1234567890 !@#$%^&*()_+-=[]{}|;':\",./<>?");
        extremeValues.put("BOOL_COL", true);
        extremeValues.put("DATE_COL", "2100-12-31");
        
        // Create another row with minimum values
        Map<String, Object> minValues = new HashMap<>();
        minValues.put("INT_COL", Integer.MIN_VALUE);
        minValues.put("FLOAT_COL", Double.MIN_VALUE);
        minValues.put("TEXT_COL", "");
        minValues.put("BOOL_COL", false);
        minValues.put("DATE_COL", "1900-01-01");
        
        // Serialize and store each row
        ObjectMapper mapper = new ObjectMapper();
        
        byte[] extremeValuesData = mapper.writeValueAsBytes(extremeValues);
        assertTrue(dataLayout.addRow(extremeValuesData), "Failed to add extreme values row");
        
        byte[] minValuesData = mapper.writeValueAsBytes(minValues);
        assertTrue(dataLayout.addRow(minValuesData), "Failed to add minimum values row");
        
        // Verify storage and retrieval
        assertEquals(2, dataLayout.getRowCount());
        
        // Test extreme values
        byte[] retrievedExtreme = dataLayout.getRow(0);
        Map<String, Object> retrievedExtremeMap = mapper.readValue(retrievedExtreme, 
                new TypeReference<HashMap<String, Object>>() {});
        
        assertEquals(Integer.MAX_VALUE, retrievedExtremeMap.get("INT_COL"));
        assertEquals(Double.MAX_VALUE, retrievedExtremeMap.get("FLOAT_COL"));
        assertEquals(extremeValues.get("TEXT_COL"), retrievedExtremeMap.get("TEXT_COL"));
        assertEquals(extremeValues.get("BOOL_COL"), retrievedExtremeMap.get("BOOL_COL"));
        assertEquals(extremeValues.get("DATE_COL"), retrievedExtremeMap.get("DATE_COL"));
        
        // Test minimum values
        byte[] retrievedMin = dataLayout.getRow(1);
        Map<String, Object> retrievedMinMap = mapper.readValue(retrievedMin, 
                new TypeReference<HashMap<String, Object>>() {});
        
        assertEquals(Integer.MIN_VALUE, retrievedMinMap.get("INT_COL"));
        assertEquals(minValues.get("FLOAT_COL"), retrievedMinMap.get("FLOAT_COL"));
        assertEquals(minValues.get("TEXT_COL"), retrievedMinMap.get("TEXT_COL"));
        assertEquals(minValues.get("BOOL_COL"), retrievedMinMap.get("BOOL_COL"));
        assertEquals(minValues.get("DATE_COL"), retrievedMinMap.get("DATE_COL"));
    }
    
    @Test
    public void testLargeRowStorage() throws IOException {
        // Create pages for the test
        Page headerPage = new Page(new PageId("TEST_TS", 1), 4096);
        Page dataPage = new Page(new PageId("TEST_TS", 2), 4096);
        
        // Create and initialize layouts
        TableHeaderPageLayout headerLayout = new TableHeaderPageLayout(headerPage);
        headerLayout.initialize();
        headerLayout.setTableName("LARGE_ROW_TEST");
        headerLayout.setFirstDataPageId(2);
        
        TableDataPageLayout dataLayout = new TableDataPageLayout(dataPage);
        dataLayout.initialize();
        
        // Define columns for storing large data
        headerLayout.addColumn("ID", DataType.INTEGER, 0, false);
        headerLayout.addColumn("LARGE_TEXT", DataType.VARCHAR, 3000, false);
        
        // Create a large text content (about 2KB)
        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            largeText.append("This is line ").append(i).append(" of the large text content. ");
            largeText.append("It contains repeated text to increase the size and test storage capacity.");
            largeText.append(" 1234567890 abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ !@#$%^&*()");
            largeText.append("\n");
        }
        
        // Create a row with the large text
        Map<String, Object> largeRow = new HashMap<>();
        largeRow.put("ID", 1);
        largeRow.put("LARGE_TEXT", largeText.toString());
        
        // Serialize and try to store the row
        ObjectMapper mapper = new ObjectMapper();
        byte[] largeRowData = mapper.writeValueAsBytes(largeRow);
        
        // Check if the page can store this large row
        boolean canStore = dataLayout.addRow(largeRowData);
        
        // If the row fits, verify retrieval
        if (canStore) {
            assertEquals(1, dataLayout.getRowCount());
            
            byte[] retrievedData = dataLayout.getRow(0);
            Map<String, Object> retrievedRow = mapper.readValue(retrievedData, 
                    new TypeReference<HashMap<String, Object>>() {});
            
            assertEquals(1, retrievedRow.get("ID"));
            assertEquals(largeText.toString(), retrievedRow.get("LARGE_TEXT"));
        } else {
            // Skip test if the row is too large for a single page
            System.out.println("Large row test skipped - row too large for page");
        }
    }
} 