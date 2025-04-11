package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import net.seitter.studiodb.storage.layout.TableDataPageLayout;

/**
 * Tests for the TableDataPageLayout class.
 */
public class TableDataPageLayoutTest {
    
    @Test
    public void testTableDataPageInitialization() {
        // Create a new page with a 4096 byte size
        Page page = new Page(new PageId("TEST_TS", 0), 4096);
        
        // Create and initialize the table data layout
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        // Test that the page is initialized with zero rows
        assertEquals(0, layout.getRowCount());
        
        // Test that free space is properly initialized
        assertEquals(4096, layout.getFreeSpaceOffset());
        assertTrue(layout.getFreeSpace() > 0);
    }
    
    @Test
    public void testAddAndRetrieveRows() {
        // Create a new page with a 4096 byte size
        Page page = new Page(new PageId("TEST_TS", 0), 4096);
        
        // Create and initialize the table data layout
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        // Create some test row data
        byte[] row1 = new byte[] {1, 2, 3, 4, 5};
        byte[] row2 = new byte[] {6, 7, 8, 9, 10};
        byte[] row3 = new byte[] {11, 12, 13, 14, 15};
        
        // Add rows
        assertTrue(layout.addRow(row1));
        assertEquals(1, layout.getRowCount());
        
        assertTrue(layout.addRow(row2));
        assertEquals(2, layout.getRowCount());
        
        assertTrue(layout.addRow(row3));
        assertEquals(3, layout.getRowCount());
        
        // Retrieve rows and verify content
        byte[] retrievedRow1 = layout.getRow(0);
        byte[] retrievedRow2 = layout.getRow(1);
        byte[] retrievedRow3 = layout.getRow(2);
        
        assertArrayEquals(row1, retrievedRow1);
        assertArrayEquals(row2, retrievedRow2);
        assertArrayEquals(row3, retrievedRow3);
        
        // Test getting all rows
        List<byte[]> allRows = layout.getAllRows();
        assertEquals(3, allRows.size());
        assertArrayEquals(row1, allRows.get(0));
        assertArrayEquals(row2, allRows.get(1));
        assertArrayEquals(row3, allRows.get(2));
    }
    
    @Test
    public void testFreeSpaceCalculation() {
        // Create a new page with a 4096 byte size
        Page page = new Page(new PageId("TEST_TS", 0), 4096);
        
        // Create and initialize the table data layout
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        int initialFreeSpace = layout.getFreeSpace();
        
        // Add a row and check free space calculation
        byte[] row = new byte[100];
        assertTrue(layout.addRow(row));
        
        int freeSpaceAfterOneRow = layout.getFreeSpace();
        assertTrue(freeSpaceAfterOneRow < initialFreeSpace);
        
        // Verify that row directory space is accounted for
        int usedSpace = initialFreeSpace - freeSpaceAfterOneRow;
        assertTrue(usedSpace >= 100 + TableDataPageLayout.ROW_DIRECTORY_ENTRY_SIZE);
    }
    
    @Test
    public void testPageFilling() {
        // Create a page with a small size to test filling
        Page page = new Page(new PageId("TEST_TS", 0), 200);
        
        // Create and initialize the table data layout
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        // Create a row that will fit
        byte[] smallRow = new byte[10];
        assertTrue(layout.addRow(smallRow));
        
        // Keep adding rows until the page is full
        int rowCount = 1;
        while (layout.getFreeSpace() >= 10 + TableDataPageLayout.ROW_DIRECTORY_ENTRY_SIZE) {
            assertTrue(layout.addRow(smallRow));
            rowCount++;
        }
        
        // Try to add one more row - should fail
        assertFalse(layout.addRow(smallRow));
        
        // Verify row count
        assertEquals(rowCount, layout.getRowCount());
    }
} 