package net.seitter.studiodb.buffer;

import net.seitter.studiodb.storage.PageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utility class to track page pins and help identify pin leaks.
 * This should be used in development/debugging only and disabled in production.
 */
public class PinTracker {
    private static final Logger logger = LoggerFactory.getLogger(PinTracker.class);
    
    // Enable/disable tracking (disable in production as it uses extra memory)
    private static final boolean TRACKING_ENABLED = true;
    
    // Maps pageId to stack trace of the most recent pin operation
    private static final Map<PageId, StackTraceElement[]> pinTraces = 
            new ConcurrentHashMap<>();
    
    // Track pin counts per page
    private static final Map<PageId, Integer> pinCounts = 
            new ConcurrentHashMap<>();
    
    /**
     * Records a pin operation for a page.
     *
     * @param pageId The ID of the page
     */
    public static void recordPin(PageId pageId) {
        if (!TRACKING_ENABLED) return;
        
        pinTraces.put(pageId, Thread.currentThread().getStackTrace());
        pinCounts.put(pageId, pinCounts.getOrDefault(pageId, 0) + 1);
    }
    
    /**
     * Records an unpin operation for a page.
     *
     * @param pageId The ID of the page
     */
    public static void recordUnpin(PageId pageId) {
        if (!TRACKING_ENABLED) return;
        
        int count = pinCounts.getOrDefault(pageId, 0) - 1;
        if (count <= 0) {
            pinCounts.remove(pageId);
            pinTraces.remove(pageId);
        } else {
            pinCounts.put(pageId, count);
        }
    }
    
    /**
     * Gets the current pin traces for all pages.
     *
     * @return A map of page IDs to stack traces
     */
    public static Map<String, String> getPinTraces() {
        if (!TRACKING_ENABLED) return new HashMap<>();
        
        Map<String, String> traces = new HashMap<>();
        
        for (Map.Entry<PageId, StackTraceElement[]> entry : pinTraces.entrySet()) {
            PageId pageId = entry.getKey();
            StackTraceElement[] stackTrace = entry.getValue();
            
            int pinCount = pinCounts.getOrDefault(pageId, 0);
            if (pinCount <= 0) continue;
            
            StringBuilder traceStr = new StringBuilder();
            traceStr.append("Pin count: ").append(pinCount).append("\n");
            
            // Skip the first two elements which are for the recordPin method itself
            for (int i = 2; i < Math.min(10, stackTrace.length); i++) {
                traceStr.append("  at ").append(stackTrace[i]).append("\n");
            }
            
            traces.put(pageId.toString(), traceStr.toString());
        }
        
        return traces;
    }
    
    /**
     * Gets pages with unusually high pin counts.
     *
     * @param threshold The threshold above which a pin count is considered high
     * @return A map of page IDs with high pin counts to their stack traces
     */
    public static Map<String, String> getHighPinCounts(int threshold) {
        if (!TRACKING_ENABLED) return new HashMap<>();
        
        Map<String, String> result = new HashMap<>();
        
        for (Map.Entry<PageId, Integer> entry : pinCounts.entrySet()) {
            if (entry.getValue() >= threshold) {
                PageId pageId = entry.getKey();
                StackTraceElement[] stackTrace = pinTraces.get(pageId);
                
                StringBuilder traceStr = new StringBuilder();
                traceStr.append("Pin count: ").append(entry.getValue()).append("\n");
                
                if (stackTrace != null) {
                    // Skip the first two elements which are for the recordPin method itself
                    for (int i = 2; i < Math.min(10, stackTrace.length); i++) {
                        traceStr.append("  at ").append(stackTrace[i]).append("\n");
                    }
                }
                
                result.put(pageId.toString(), traceStr.toString());
            }
        }
        
        return result;
    }
    
    /**
     * Clears all pin tracking data.
     */
    public static void clear() {
        if (!TRACKING_ENABLED) return;
        
        pinTraces.clear();
        pinCounts.clear();
    }
    
    /**
     * Adds a command to show high pin counts.
     *
     * @return String representation of pages with high pin counts
     */
    public static String debugHighPinCounts() {
        Map<String, String> highPins = getHighPinCounts(3);
        
        if (highPins.isEmpty()) {
            return "No pages with high pin counts detected.";
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append("Pages with high pin counts:\n\n");
        
        for (Map.Entry<String, String> entry : highPins.entrySet()) {
            sb.append("PageId: ").append(entry.getKey()).append("\n");
            sb.append(entry.getValue()).append("\n");
        }
        
        return sb.toString();
    }
} 