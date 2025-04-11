package net.seitter.studiodb.storage;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import net.seitter.studiodb.storage.layout.ContainerMetadataPageLayout;
import net.seitter.studiodb.storage.layout.FreeSpaceMapPageLayout;
import net.seitter.studiodb.storage.layout.PageLayoutFactory;

/**
 * Tests for the Container Metadata and Free Space Map page layouts.
 */
public class MetadataPageLayoutTest {
    
    @Test
    public void testContainerMetadataPageLayout() {
        // Create a new page with a 4096 byte size
        Page page = new Page(new PageId("TEST_TS", 0), 4096);
        
        // Create and initialize the metadata layout
        ContainerMetadataPageLayout layout = new ContainerMetadataPageLayout(page);
        layout.initialize();
        
        // Test getting/setting tablespace name
        layout.setTablespaceName("TEST_TABLESPACE");
        assertEquals("TEST_TABLESPACE", layout.getTablespaceName());
        
        // Test getting/setting page size
        layout.setPageSize(8192);
        assertEquals(8192, layout.getPageSize());
        
        // Test getting/setting creation time
        long currentTime = System.currentTimeMillis();
        layout.setCreationTime(currentTime);
        assertEquals(currentTime, layout.getCreationTime());
        
        // Test getting/setting last opened time
        long lastOpenedTime = System.currentTimeMillis() + 1000; // 1 second later
        layout.setLastOpenedTime(lastOpenedTime);
        assertEquals(lastOpenedTime, layout.getLastOpenedTime());
        
        // Test getting/setting total pages
        layout.setTotalPages(100);
        assertEquals(100, layout.getTotalPages());
        
        // Test getting/setting free space map page ID
        layout.setFreeSpaceMapPageId(1);
        assertEquals(1, layout.getFreeSpaceMapPageId());
        
        // Test with long tablespace name
        String longName = "THIS_IS_A_VERY_LONG_TABLESPACE_NAME_THAT_EXCEEDS_THE_MAXIMUM_ALLOWED_LENGTH_FOR_TESTING_TRUNCATION";
        layout.setTablespaceName(longName);
        String retrievedName = layout.getTablespaceName();
        assertTrue(retrievedName.length() <= 64, "Tablespace name should be truncated to 64 characters");
        assertEquals(longName.substring(0, retrievedName.length()), retrievedName);
    }
    
    @Test
    public void testFreeSpaceMapPageLayout() {
        // Create a new page with a 4096 byte size
        Page page = new Page(new PageId("TEST_TS", 1), 4096);
        
        // Create and initialize the free space map layout
        FreeSpaceMapPageLayout layout = new FreeSpaceMapPageLayout(page);
        layout.initialize();
        
        // Verify capacity
        int capacity = layout.getBitmapCapacity();
        assertTrue(capacity > 0);
        System.out.println("Free space map capacity: " + capacity + " pages");
        
        // Verify first two pages are marked as used by default (page 0 and 1)
        assertFalse(layout.isPageFree(0), "Page 0 should be marked as used");
        assertFalse(layout.isPageFree(1), "Page 1 should be marked as used");
        
        // Test marking pages as free/used
        layout.markPageAsFree(2);
        layout.markPageAsFree(3);
        layout.markPageAsFree(4);
        
        assertTrue(layout.isPageFree(2), "Page 2 should be free");
        assertTrue(layout.isPageFree(3), "Page 3 should be free");
        assertTrue(layout.isPageFree(4), "Page 4 should be free");
        
        layout.markPageAsUsed(3);
        
        assertTrue(layout.isPageFree(2), "Page 2 should still be free");
        assertFalse(layout.isPageFree(3), "Page 3 should now be used");
        assertTrue(layout.isPageFree(4), "Page 4 should still be free");
        
        // Test allocating a page
        int allocatedPage = layout.allocatePage();
        assertTrue(allocatedPage == 2 || allocatedPage == 4, "Should allocate either page 2 or 4");
        assertFalse(layout.isPageFree(allocatedPage), "Allocated page should be marked as used");
        
        // Allocate another page
        int secondAllocated = layout.allocatePage();
        assertTrue((secondAllocated == 2 || secondAllocated == 4) && secondAllocated != allocatedPage, 
                   "Should allocate the remaining free page (2 or 4)");
        assertFalse(layout.isPageFree(secondAllocated), "Second allocated page should be marked as used");
        
        // Mark more pages as free
        layout.markPageAsFree(5);
        layout.markPageAsFree(6);
        
        // Test counting free pages
        assertEquals(2, layout.countFreePages(), "Should have exactly 2 free pages (5 and 6)");
        
        // Test wrapping around in free page search
        layout.setLastCheckedPage(capacity - 1); // Set to end of bitmap
        int wrappedPage = layout.findNextFreePage();
        assertTrue(wrappedPage == 5 || wrappedPage == 6, "Should find page 5 or 6 after wrapping");
    }
    
    @Test
    public void testPageLayoutFactory() {
        // Test creating container metadata page
        PageId metadataPageId = new PageId("TEST_TS", 0);
        PageLayout metadataLayout = PageLayoutFactory.createNewPage(metadataPageId, 4096, PageType.CONTAINER_METADATA);
        
        assertNotNull(metadataLayout, "Should create a non-null metadata layout");
        assertTrue(metadataLayout instanceof ContainerMetadataPageLayout, 
                   "Should create a ContainerMetadataPageLayout instance");
        
        // Test creating free space map page
        PageId fsmPageId = new PageId("TEST_TS", 1);
        PageLayout fsmLayout = PageLayoutFactory.createNewPage(fsmPageId, 4096, PageType.FREE_SPACE_MAP);
        
        assertNotNull(fsmLayout, "Should create a non-null free space map layout");
        assertTrue(fsmLayout instanceof FreeSpaceMapPageLayout, 
                   "Should create a FreeSpaceMapPageLayout instance");
        
        // Test initializing a pre-existing page
        Page page = new Page(new PageId("TEST_TS", 2), 4096);
        FreeSpaceMapPageLayout customLayout = new FreeSpaceMapPageLayout(page);
        customLayout.initialize();
        
        // Verify it was initialized correctly
        assertFalse(customLayout.isPageFree(0), "Page 0 should be marked as used");
        assertFalse(customLayout.isPageFree(1), "Page 1 should be marked as used");
    }
} 