package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import net.seitter.studiodb.storage.layout.TableDataPageLayout;
import net.seitter.studiodb.storage.layout.PageLayoutFactory;

/**
 * Tests for the Page and PageLayout classes with the new single magic number design.
 */
public class PageLayoutTest {
    
    // Concrete implementation of PageLayout for testing
    private static class TestPageLayout extends PageLayout {
        private final PageType pageType;
        private static final int ROW_COUNT_OFFSET = PageLayout.HEADER_SIZE;
        
        public TestPageLayout(Page page, PageType pageType) {
            super(page);
            this.pageType = pageType;
        }
        
        @Override
        public void initialize() {
            writeHeader(pageType);
            // Set free space to start at the end of the buffer, like TableDataPageLayout does
            setFreeSpaceOffset(page.getPageSize());
        }
        
        // Add row count methods that were moved out of PageLayout
        public void setRowCount(int rowCount) {
            buffer.putInt(ROW_COUNT_OFFSET, rowCount);
            page.markDirty();
        }
        
        public int getRowCount() {
            return buffer.getInt(ROW_COUNT_OFFSET);
        }
    }
    
    @Test
    void testPageLayoutHeader() {
        // Create a new page with a 1024 byte size
        Page page = new Page(new PageId("TEST_TS", 1), 1024);
        
        // Create and initialize the test layout
        TestPageLayout layout = new TestPageLayout(page, PageType.TABLE_DATA);
        layout.initialize();
        
        // Verify header was written correctly
        assertEquals(PageType.TABLE_DATA, layout.readHeader());
        assertEquals(-1, layout.getNextPageId());
        assertEquals(-1, layout.getPrevPageId());
        assertEquals(0, layout.getRowCount());
        // For TableDataPageLayout, free space is set to buffer capacity, not HEADER_SIZE
        assertEquals(layout.getPage().getPageSize(), layout.getFreeSpaceOffset());
        
        // Test setting header values
        layout.setNextPageId(2);
        layout.setPrevPageId(3);
        layout.setRowCount(5);
        layout.setFreeSpaceOffset(100);
        
        assertEquals(2, layout.getNextPageId());
        assertEquals(3, layout.getPrevPageId());
        assertEquals(5, layout.getRowCount());
        assertEquals(100, layout.getFreeSpaceOffset());
        
        // Test free space calculation
        assertEquals(1024 - layout.getFreeSpaceOffset(), layout.getFreeSpace());
    }
    
    @Test
    public void testPageLayoutFactory() {
        // Create a new page
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize a TableDataPageLayout directly
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        // Verify the page header was written correctly using the PageLayout's own methods
        PageType pageType = layout.readHeader();
        assertEquals(PageType.TABLE_DATA, pageType, "Page type should match");
        assertEquals(-1, layout.getNextPageId(), "Next page ID should be -1");
        assertEquals(-1, layout.getPrevPageId(), "Previous page ID should be -1");
        assertEquals(0, layout.getRowCount(), "Row count should be 0");
        assertEquals(page.getPageSize(), layout.getFreeSpaceOffset(), "Free space offset should be at the end of the buffer");
        
        // Also verify the buffer content directly
        ByteBuffer buffer = page.getBuffer();
        
        // Dump first 32 bytes of buffer for debugging
        System.out.println("Buffer contents (first 32 bytes):");
        buffer.position(0);
        for (int i = 0; i < 32; i++) {
            System.out.printf("%02X ", buffer.get() & 0xFF);
            if ((i + 1) % 16 == 0) System.out.println();
        }
        System.out.println();
        
        // Check first byte for page type
        buffer.position(0);
        int typeId = buffer.get() & 0xFF;
        System.out.println("Expected type ID: " + PageType.TABLE_DATA.getTypeId() + ", Actual: " + typeId);
        assertEquals(PageType.TABLE_DATA.getTypeId(), typeId, "Page type should match");
    }
    
    @Test
    public void testInvalidMagicNumber() {
        // Create a new page
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Write an invalid magic number
        ByteBuffer buffer = page.getBuffer();
        buffer.position(1); // Skip page type
        buffer.putInt(0xDEADBEEF); // Invalid magic number
        
        // Try to create a layout - should return null
        PageLayout layout = PageLayoutFactory.createLayout(page);
        assertNull(layout, "Layout should be null for invalid magic number");
    }
    
    @Test
    public void testPageTypeEnum() {
        // Test all page types
        for (PageType type : PageType.values()) {
            // Verify type ID is unique
            assertEquals(type, PageType.fromTypeId(type.getTypeId()), 
                    "Type ID should map back to correct enum value");
        }
        
        // Test invalid type ID
        assertThrows(IllegalArgumentException.class, () -> {
            PageType.fromTypeId(-1);
        }, "Should throw exception for invalid type ID");
    }
    
    @Test
    public void testTableDataLayoutAddRow() {
        // Create a test page
        PageId pageId = new PageId("test", 1);
        Page page = new Page(pageId, 1024);
        
        // Create layout
        TableDataPageLayout layout = new TableDataPageLayout(page);
        layout.initialize();
        
        // Add rows
        byte[] row1 = new byte[100];
        for (int i = 0; i < row1.length; i++) {
            row1[i] = (byte) i;
        }
        
        assertTrue(layout.addRow(row1));
        
        // Verify row was added
        byte[] retrievedRow = layout.getRow(0);
        assertNotNull(retrievedRow);
        assertEquals(row1.length, retrievedRow.length);
        assertArrayEquals(row1, retrievedRow);
        
        // Verify row count was updated
        assertEquals(1, layout.getRowCount());
        
        // Add another row
        byte[] row2 = new byte[200];
        for (int i = 0; i < row2.length; i++) {
            row2[i] = (byte) (i + 10);
        }
        
        assertTrue(layout.addRow(row2));
        
        // Verify second row was added
        retrievedRow = layout.getRow(1);
        assertNotNull(retrievedRow);
        assertEquals(row2.length, retrievedRow.length);
        assertArrayEquals(row2, retrievedRow);
        
        // Verify row count was updated
        assertEquals(2, layout.getRowCount());
    }
} 