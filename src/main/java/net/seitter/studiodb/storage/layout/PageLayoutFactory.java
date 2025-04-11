package net.seitter.studiodb.storage.layout;

import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import java.nio.ByteBuffer;

/**
 * Factory class for creating page layouts.
 * Determines the appropriate layout class based on the page type.
 */
public class PageLayoutFactory {
    
    /**
     * Creates a page layout for the given page.
     * 
     * @param page The page to create a layout for
     * @return The appropriate page layout, or null if the page type is unknown
     */
    public static PageLayout createLayout(Page page) {
        ByteBuffer buffer = page.getBuffer();
        
        if (buffer.limit() < PageLayout.HEADER_SIZE) {
            return null;
        }
        
        // Read page type and magic number
        int typeId = buffer.get(0) & 0xFF;
        int magic = buffer.getInt(1);
        
        // Get page type
        PageType pageType;
        try {
            pageType = PageType.fromTypeId(typeId);
        } catch (IllegalArgumentException e) {
            return null; // Invalid page type
        }
        
        // Validate magic number - now we use only the standard MAGIC_NUMBER
        if (magic != PageLayout.MAGIC_NUMBER) {
            return null;
        }
        
        // Create the appropriate layout based on page type
        switch (pageType) {
            case TABLE_HEADER:
                return new TableHeaderPageLayout(page);
            case TABLE_DATA:
                return new TableDataPageLayout(page);
            case INDEX_HEADER:
            case INDEX_INTERNAL:
            case INDEX_LEAF:
                return new IndexPageLayout(page);
            case CONTAINER_METADATA:
                return new ContainerMetadataPageLayout(page);
            case FREE_SPACE_MAP:
                return new FreeSpaceMapPageLayout(page);
            default:
                return null;
        }
    }
    
    /**
     * Creates a new page with the specified layout.
     * 
     * @param pageId The page ID
     * @param pageSize The page size
     * @param pageType The type of page to create
     * @return The page layout, or null if the page type is unknown
     */
    public static PageLayout createNewPage(PageId pageId, int pageSize, PageType pageType) {
        Page page = new Page(pageId, pageSize);
        
        // Create the appropriate layout
        PageLayout layout = null;
        switch (pageType) {
            case TABLE_HEADER:
                layout = new TableHeaderPageLayout(page);
                break;
            case TABLE_DATA:
                layout = new TableDataPageLayout(page);
                break;
            case INDEX_HEADER:
            case INDEX_INTERNAL:
            case INDEX_LEAF:
                IndexPageLayout indexLayout = new IndexPageLayout(page);
                indexLayout.initialize(pageType, DataType.INTEGER); // Initialize with default key type
                return indexLayout;
            case CONTAINER_METADATA:
                layout = new ContainerMetadataPageLayout(page);
                break;
            case FREE_SPACE_MAP:
                layout = new FreeSpaceMapPageLayout(page);
                break;
            default:
                return null;
        }
        
        // Initialize the layout with the specified page type
        layout.initialize();
        return layout;
    }
} 