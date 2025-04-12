package net.seitter.studiodb.storage.layout;

import net.seitter.studiodb.schema.DataType;
import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.storage.PageLayout;
import net.seitter.studiodb.storage.PageType;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory class for creating page layouts.
 * Determines the appropriate layout class based on the page type.
 */
public class PageLayoutFactory {
    private static final Logger logger = LoggerFactory.getLogger(PageLayoutFactory.class);
    
    /**
     * Creates a page layout for the given page.
     * 
     * @param page The page to create a layout for
     * @return The appropriate page layout, or null if the page type is unknown
     */
    public static PageLayout createLayout(Page page) {
        ByteBuffer buffer = page.getBuffer();
        
        if (buffer.limit() < PageLayout.HEADER_SIZE) {
            logger.error("Page buffer too small for header: {} bytes, need at least {}", 
                    buffer.limit(), PageLayout.HEADER_SIZE);
            return null;
        }
        
        // Read page type and magic number
        int typeId = buffer.get(0) & 0xFF;
        int magic = buffer.getInt(1);
        
        // Log details about the page being created
        PageId pageId = page.getPageId();
        String pageDesc = (pageId != null) ? 
            String.format("Page %d in %s", pageId.getPageNumber(), pageId.getTablespaceName()) : 
            "Unknown page";
        
        // Validate magic number - now we use only the standard MAGIC_NUMBER
        if (magic != PageLayout.MAGIC_NUMBER) {
            logger.error("{}: Invalid magic number: expected 0x{}, found 0x{}", 
                    pageDesc, Integer.toHexString(PageLayout.MAGIC_NUMBER), Integer.toHexString(magic));
            return null;
        }
        
        // Get page type
        PageType pageType;
        try {
            pageType = PageType.fromTypeId(typeId);
        } catch (IllegalArgumentException e) {
            logger.error("{}: Invalid page type ID: {}", pageDesc, typeId);
            return null; // Invalid page type
        }
        
        logger.debug("{}: Creating layout for page type: {}", pageDesc, pageType);
        
        // Create the appropriate layout based on page type
        PageLayout layout = null;
        try {
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
                case CONTAINER_METADATA:
                    layout = new ContainerMetadataPageLayout(page);
                    break;
                case FREE_SPACE_MAP:
                    layout = new FreeSpaceMapPageLayout(page);
                    break;
                default:
                    logger.warn("{}: Unhandled page type: {}", pageDesc, pageType);
                    return null;
            }
            
            // Verify the layout was created successfully
            if (layout != null) {
                // Additional validation: try to read the page type to ensure it's valid
                try {
                    PageType readType = layout.getPageType();
                    if (readType != pageType) {
                        logger.warn("{}: Page type mismatch: expected {}, read {}", 
                                pageDesc, pageType, readType);
                    }
                } catch (Exception e) {
                    logger.error("{}: Failed to validate page type: {}", pageDesc, e.getMessage());
                    return null;
                }
            }
            
            return layout;
        } catch (Exception e) {
            logger.error("{}: Error creating layout for page type {}: {}", 
                    pageDesc, pageType, e.getMessage());
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
        String pageDesc = String.format("New page %d in %s", pageId.getPageNumber(), pageId.getTablespaceName());
        
        logger.debug("{}: Creating new page with type: {}", pageDesc, pageType);
        
        // Create the appropriate layout
        PageLayout layout = null;
        try {
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
                    logger.warn("{}: Unhandled page type for new page: {}", pageDesc, pageType);
                    return null;
            }
            
            // Initialize the layout with the specified page type
            if (layout != null) {
                layout.initialize();
                
                // Verify initialization succeeded
                try {
                    PageType readType = layout.getPageType();
                    if (readType != pageType) {
                        logger.warn("{}: New page type mismatch after initialization: expected {}, read {}",
                                pageDesc, pageType, readType);
                    }
                } catch (Exception e) {
                    logger.error("{}: Failed to validate new page initialization: {}", pageDesc, e.getMessage());
                    return null;
                }
            }
            return layout;
        } catch (Exception e) {
            logger.error("{}: Error creating new page with type {}: {}", 
                    pageDesc, pageType, e.getMessage());
            return null;
        }
    }
} 