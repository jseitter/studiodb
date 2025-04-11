package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import net.seitter.studiodb.storage.layout.FreeSpaceMapPageLayout;

/**
 * Dedicated tests for the FreeSpaceMapPageLayout class.
 */
public class FreeSpaceMapPageLayoutTest {
    
    private FreeSpaceMapPageLayout layout;
    private Page page;
    
    @BeforeEach
    public void setup() {
        // Create a new page with a 4096 byte size
        page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the free space map layout
        layout = new FreeSpaceMapPageLayout(page);
        layout.initialize();
    }
    
    @Test
    public void testInitialization() {
        // Verify the page type is correct
        assertEquals(PageType.FREE_SPACE_MAP, layout.readHeader());
        
        // Verify capacity
        int capacity = layout.getBitmapCapacity();
        assertTrue(capacity > 0, "Bitmap capacity should be greater than 0");
        
        // Verify first two pages are marked as used by default (page 0 and 1)
        assertFalse(layout.isPageFree(0), "Page 0 should be marked as used");
        assertFalse(layout.isPageFree(1), "Page 1 should be marked as used");
        
        // The rest should be free initially (because clearBitmap sets all to 0/used, then
        // initialize() marks 0 and 1 as used explicitly)
        assertFalse(layout.isPageFree(2), "Page 2 should be marked as used initially");
        assertFalse(layout.isPageFree(3), "Page 3 should be marked as used initially");
        
        // Mark a few pages as free to test
        layout.markPageAsFree(2);
        layout.markPageAsFree(3);
        assertTrue(layout.isPageFree(2), "Page 2 should now be free");
        assertTrue(layout.isPageFree(3), "Page 3 should now be free");
    }
    
    @Test
    public void testMarkPageFreeAndUsed() {
        // Mark several pages as free
        layout.markPageAsFree(2);
        layout.markPageAsFree(3);
        layout.markPageAsFree(4);
        
        assertTrue(layout.isPageFree(2), "Page 2 should be free after marking");
        assertTrue(layout.isPageFree(3), "Page 3 should be free after marking");
        assertTrue(layout.isPageFree(4), "Page 4 should be free after marking");
        
        // Mark a page as used
        layout.markPageAsUsed(3);
        
        assertTrue(layout.isPageFree(2), "Page 2 should still be free");
        assertFalse(layout.isPageFree(3), "Page 3 should now be used");
        assertTrue(layout.isPageFree(4), "Page 4 should still be free");
    }
    
    @Test
    public void testAllocatePage() {
        // Mark several pages as free
        layout.markPageAsFree(2);
        layout.markPageAsFree(3);
        
        // Allocate a page
        int allocatedPage = layout.allocatePage();
        assertTrue(allocatedPage == 2 || allocatedPage == 3, 
                   "Should allocate either page 2 or 3");
        assertFalse(layout.isPageFree(allocatedPage), 
                    "Allocated page should be marked as used");
        
        // Allocate another page
        int secondAllocated = layout.allocatePage();
        assertTrue((secondAllocated == 2 || secondAllocated == 3) && 
                   secondAllocated != allocatedPage, 
                   "Should allocate the remaining free page (2 or 3)");
        assertFalse(layout.isPageFree(secondAllocated), 
                    "Second allocated page should be marked as used");
        
        // Try to allocate when no pages are free
        assertEquals(-1, layout.allocatePage(), 
                    "Should return -1 when no free pages are available");
    }
    
    @Test
    public void testFindNextFreePage() {
        // Mark specific pages as free
        layout.markPageAsFree(5);
        layout.markPageAsFree(10);
        layout.markPageAsFree(15);
        
        // Start from the beginning
        layout.setLastCheckedPage(0);
        assertEquals(5, layout.findNextFreePage(), "Should find page 5");
        
        // Start from after 5
        layout.setLastCheckedPage(6);
        assertEquals(10, layout.findNextFreePage(), "Should find page 10");
        
        // Start from after 10
        layout.setLastCheckedPage(11);
        assertEquals(15, layout.findNextFreePage(), "Should find page 15");
        
        // Start from after 15, should wrap around to 5
        layout.setLastCheckedPage(16);
        assertEquals(5, layout.findNextFreePage(), "Should wrap around and find page 5");
    }
    
    @Test
    public void testCountFreePages() {
        // Initially all pages are marked as used (0) due to clearBitmap()
        assertEquals(0, layout.countFreePages(), "Initially no pages should be free");
        
        // Mark some pages as free
        layout.markPageAsFree(2);
        layout.markPageAsFree(3);
        layout.markPageAsFree(4);
        
        // Count should match number of marked pages
        assertEquals(3, layout.countFreePages(), "Free page count should be 3");
        
        // Mark one page as used again
        layout.markPageAsUsed(3);
        
        // Count should decrease
        assertEquals(2, layout.countFreePages(), "Free page count should be 2");
    }
    
    @Test
    public void testBitmapCapacity() {
        // Get bitmap capacity 
        int capacity = layout.getBitmapCapacity();
        
        // It should be based on page size, likely thousands of pages for 4KB page
        assertTrue(capacity > 1000, "Bitmap capacity should be large for 4KB page");
        
        // Test edge cases - last page in capacity
        layout.markPageAsFree(capacity - 1);
        assertTrue(layout.isPageFree(capacity - 1), 
                  "Last page in capacity range should be accessible");
        
        // Page id beyond capacity should return false, not throw exception
        assertFalse(layout.isPageFree(capacity + 1), 
                   "Page id beyond capacity should return false");
        
        // Marking beyond capacity should return false
        assertFalse(layout.markPageAsFree(capacity + 1), 
                   "Marking page beyond capacity should return false");
        assertFalse(layout.markPageAsUsed(capacity + 1), 
                   "Marking page beyond capacity should return false");
    }
    
    @Test
    public void testBoundaryConditions() {
        // Test negative page IDs - these should return false, not throw exceptions
        assertFalse(layout.isPageFree(-1), 
                    "Negative page id should return false, not throw");
        
        assertFalse(layout.markPageAsUsed(-1), 
                    "Negative page id should return false for markPageAsUsed");
        
        assertFalse(layout.markPageAsFree(-1), 
                    "Negative page id should return false for markPageAsFree");
    }
    
    @Test
    public void testNextPageId() {
        // Test setting and getting next page ID
        assertEquals(-1, layout.getNextPageId(), "Default next page ID should be -1");
        
        layout.setNextPageId(5);
        assertEquals(5, layout.getNextPageId(), "Next page ID should be 5 after setting");
    }
} 