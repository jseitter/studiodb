package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.Page;
import net.seitter.studiodb.storage.PageId;
import net.seitter.studiodb.web.WebServer;
import net.seitter.studiodb.web.model.PageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * Instrumenter for BufferPoolManager that captures page events for visualization.
 * Uses the Proxy pattern to intercept method calls on the BufferPoolManager.
 */
public class BufferPoolManagerInstrumenter {
    private static final Logger logger = LoggerFactory.getLogger(BufferPoolManagerInstrumenter.class);
    private final IBufferPoolManager original;
    private final WebServer webServer;

    /**
     * Creates a new instrumenter.
     *
     * @param original The original buffer pool manager
     * @param webServer The web server to send events to
     */
    public BufferPoolManagerInstrumenter(IBufferPoolManager original, WebServer webServer) {
        this.original = original;
        this.webServer = webServer;
    }

    /**
     * Creates a proxy BufferPoolManager that captures events for visualization.
     *
     * @return A proxy BufferPoolManager
     */
    public IBufferPoolManager createProxy() {
        return (IBufferPoolManager) Proxy.newProxyInstance(
                IBufferPoolManager.class.getClassLoader(),
                new Class<?>[]{IBufferPoolManager.class},
                new BufferPoolManagerHandler(original, webServer));
    }

    /**
     * Invocation handler for BufferPoolManager methods.
     */
    private static class BufferPoolManagerHandler implements InvocationHandler {
        private final IBufferPoolManager target;
        private final WebServer webServer;

        public BufferPoolManagerHandler(IBufferPoolManager target, WebServer webServer) {
            this.target = target;
            this.webServer = webServer;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();
            
            // Intercept methods to capture events
            try {
                if ("fetchPage".equals(methodName) && args != null && args.length > 0) {
                    return handleFetchPage((PageId) args[0]);
                } else if ("unpinPage".equals(methodName) && args != null && args.length > 1) {
                    return handleUnpinPage((PageId) args[0], (Boolean) args[1]);
                } else if ("flushAll".equals(methodName)) {
                    return handleFlushAll();
                } else if ("allocatePage".equals(methodName)) {
                    return handleAllocatePage();
                } else {
                    // Default behavior for other methods
                    return method.invoke(target, args);
                }
            } catch (Exception e) {
                logger.error("Error in buffer pool manager instrumentation", e);
                throw e;
            }
        }

        /**
         * Handles fetchPage method calls.
         */
        private Page handleFetchPage(PageId pageId) throws IOException {
            // Execute the original method
            Page page = target.fetchPage(pageId);
            
            // Capture the event if the page was successfully fetched
            if (page != null) {
                PageEvent event = new PageEvent(
                        pageId.getTablespaceName(),
                        pageId.getPageNumber(),
                        PageEvent.EventType.PAGE_READ,
                        "Page read from disk to buffer pool");
                
                webServer.addPageEvent(event);
                
                // Also capture a pin event
                PageEvent pinEvent = new PageEvent(
                        pageId.getTablespaceName(),
                        pageId.getPageNumber(),
                        PageEvent.EventType.PAGE_PIN,
                        "Page pinned in buffer pool");
                
                webServer.addPageEvent(pinEvent);
            }
            
            return page;
        }

        /**
         * Handles unpinPage method calls.
         */
        private Object handleUnpinPage(PageId pageId, boolean isDirty) {
            // Execute the original method
            target.unpinPage(pageId, isDirty);
            
            // Capture the unpin event
            PageEvent unpinEvent = new PageEvent(
                    pageId.getTablespaceName(),
                    pageId.getPageNumber(),
                    PageEvent.EventType.PAGE_UNPIN,
                    "Page unpinned in buffer pool");
            
            webServer.addPageEvent(unpinEvent);
            
            // If the page was marked dirty, capture a dirty event
            if (isDirty) {
                PageEvent dirtyEvent = new PageEvent(
                        pageId.getTablespaceName(),
                        pageId.getPageNumber(),
                        PageEvent.EventType.PAGE_DIRTY,
                        "Page marked as dirty");
                
                webServer.addPageEvent(dirtyEvent);
            }
            
            return null;
        }

        /**
         * Handles flushAll method calls.
         */
        private Object handleFlushAll() throws IOException {
            // Execute the original method
            target.flushAll();
            
            // Capture a generic event for the flush operation
            PageEvent event = new PageEvent(
                    target.getTablespaceName(),
                    -1, // Not specific to any page
                    PageEvent.EventType.BUFFER_FLUSH,
                    "All dirty pages flushed to disk");
            
            webServer.addPageEvent(event);
            
            return null;
        }

        /**
         * Handles allocatePage method calls.
         */
        private Page handleAllocatePage() throws IOException {
            // Execute the original method
            Page page = target.allocatePage();
            
            // Capture the event if the page was successfully allocated
            if (page != null) {
                PageId pageId = page.getPageId();
                
                PageEvent event = new PageEvent(
                        pageId.getTablespaceName(),
                        pageId.getPageNumber(),
                        PageEvent.EventType.PAGE_ALLOCATE,
                        "New page allocated");
                
                webServer.addPageEvent(event);
                
                // Also capture a pin event
                PageEvent pinEvent = new PageEvent(
                        pageId.getTablespaceName(),
                        pageId.getPageNumber(),
                        PageEvent.EventType.PAGE_PIN,
                        "Page pinned in buffer pool");
                
                webServer.addPageEvent(pinEvent);
            }
            
            return page;
        }
    }
} 