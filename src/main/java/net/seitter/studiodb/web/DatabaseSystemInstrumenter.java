package net.seitter.studiodb.web;

import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.buffer.BufferPoolManagerInstrumenter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * Instrumenter for the database system.
 * Instruments the components of the database system to capture events for visualization.
 */
public class DatabaseSystemInstrumenter {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseSystemInstrumenter.class);
    private final DatabaseSystem dbSystem;
    private final WebServer webServer;

    /**
     * Creates a new instrumenter.
     *
     * @param dbSystem The database system to instrument
     * @param webServer The web server to send events to
     */
    public DatabaseSystemInstrumenter(DatabaseSystem dbSystem, WebServer webServer) {
        this.dbSystem = dbSystem;
        this.webServer = webServer;
    }

    /**
     * Instruments the database system components.
     */
    public void instrumentComponents() {
        logger.info("Instrumenting database system components for visualization");
        
        try {
            // Instrument buffer pool managers
            instrumentBufferPoolManagers();
            
            // TODO: Instrument storage manager
            // TODO: Instrument schema manager
            
            logger.info("Successfully instrumented database system components");
        } catch (Exception e) {
            logger.error("Failed to instrument database system components", e);
        }
    }

    /**
     * Instruments the buffer pool managers.
     */
    @SuppressWarnings("unchecked")
    private void instrumentBufferPoolManagers() throws Exception {
        // Use reflection to access the private bufferPoolManagers field
        Field bufferPoolManagersField = DatabaseSystem.class.getDeclaredField("bufferPoolManagers");
        bufferPoolManagersField.setAccessible(true);
        
        Map<String, BufferPoolManager> originalManagers = 
                (Map<String, BufferPoolManager>) bufferPoolManagersField.get(dbSystem);
        
        // Create proxies for all buffer pool managers
        Map<String, BufferPoolManager> proxiedManagers = new HashMap<>();
        
        for (Map.Entry<String, BufferPoolManager> entry : originalManagers.entrySet()) {
            String name = entry.getKey();
            BufferPoolManager original = entry.getValue();
            
            // Create a proxy
            BufferPoolManagerInstrumenter instrumenter = 
                    new BufferPoolManagerInstrumenter(original, webServer);
            BufferPoolManager proxy = instrumenter.createProxy();
            
            proxiedManagers.put(name, proxy);
            logger.debug("Instrumented buffer pool manager for tablespace '{}'", name);
        }
        
        // Replace the original managers with the proxied ones
        bufferPoolManagersField.set(dbSystem, proxiedManagers);
        logger.info("Replaced {} buffer pool managers with instrumented versions", proxiedManagers.size());
    }
} 