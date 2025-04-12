package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility methods for working with the buffer pool in a safe way.
 * These methods ensure pages are properly unpinned in all code paths.
 */
public class BufferPoolUtils {
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolUtils.class);
    
    /**
     * Functional interface for operations on a page.
     * 
     * @param <T> The return type of the operation
     */
    public interface PageOperation<T> {
        /**
         * Executes an operation on a page.
         * 
         * @param page The page to operate on
         * @return The result of the operation
         * @throws IOException If an I/O error occurs
         */
        T execute(Page page) throws IOException;
    }
    
    /**
     * Safely performs an operation on a page, ensuring the page is unpinned
     * regardless of whether the operation succeeds or throws an exception.
     * 
     * @param <T> The return type of the operation
     * @param bufferPool The buffer pool to use
     * @param pageId The ID of the page to operate on
     * @param markDirty Whether to mark the page as dirty after the operation
     * @param operation The operation to perform on the page
     * @return The result of the operation, or null if the page couldn't be fetched
     * @throws IOException If an I/O error occurs
     */
    public static <T> T withPage(IBufferPoolManager bufferPool, PageId pageId, 
                                boolean markDirty, PageOperation<T> operation) throws IOException {
        Page page = null;
        try {
            page = bufferPool.fetchPage(pageId);
            if (page == null) {
                return null;
            }
            
            T result = operation.execute(page);
            
            if (markDirty) {
                page.markDirty();
            }
            
            return result;
        } finally {
            if (page != null) {
                bufferPool.unpinPage(pageId, page.isDirty() || markDirty);
            }
        }
    }
    
    /**
     * Safely executes a chain of operations on multiple pages, ensuring all pages are 
     * properly unpinned even if operations fail.
     * 
     * @param <T> The return type of the operation chain
     * @param bufferPool The buffer pool to use
     * @param operation The operation chain to execute
     * @return The result of the operation chain
     * @throws IOException If an I/O error occurs
     */
    public static <T> T withSafeOperations(IBufferPoolManager bufferPool,
                                         SafePageOperationChain<T> operation) throws IOException {
        return operation.execute(bufferPool);
    }
    
    /**
     * Functional interface for a chain of page operations.
     * 
     * @param <T> The return type of the operation chain
     */
    public interface SafePageOperationChain<T> {
        /**
         * Executes a chain of operations using the provided buffer pool.
         * 
         * @param bufferPool The buffer pool to use
         * @return The result of the operation chain
         * @throws IOException If an I/O error occurs
         */
        T execute(IBufferPoolManager bufferPool) throws IOException;
    }
} 