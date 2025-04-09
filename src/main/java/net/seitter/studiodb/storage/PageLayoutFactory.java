package net.seitter.studiodb.storage;

import net.seitter.studiodb.schema.DataType;
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
        
        // Validate magic number
        if (magic != PageLayout.MAGIC_NUMBER) {
            return null;
        }
        
        PageType pageType = PageType.fromTypeId(typeId);
        
        switch (pageType) {
            case TABLE_HEADER:
                return new TableHeaderPageLayout(page);
            case TABLE_DATA:
                return new TableDataPageLayout(page);
            case INDEX_HEADER:
            case INDEX_INTERNAL:
            case INDEX_LEAF:
                return new IndexPageLayout(page);
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
                layout = new IndexPageLayout(page);
                break;
            default:
                return null;
        }
        
        // Initialize the layout with the specified page type
        layout.initialize();
        return layout;
    }
} 