package net.seitter.studiodb.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.json.JavalinJackson;
import io.javalin.plugin.bundled.CorsPluginConfig;
import io.javalin.websocket.WsContext;
import net.seitter.studiodb.DatabaseSystem;
import net.seitter.studiodb.buffer.BufferPoolManager;
import net.seitter.studiodb.storage.StorageManager;
import net.seitter.studiodb.storage.Tablespace;
import net.seitter.studiodb.web.model.BufferPoolStatus;
import net.seitter.studiodb.web.model.PageEvent;
import net.seitter.studiodb.web.model.PageStatus;
import net.seitter.studiodb.web.model.TablespaceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Web server for the visualization interface.
 * Provides REST API endpoints and serves the React frontend.
 */
public class WebServer {
    private static final Logger logger = LoggerFactory.getLogger(WebServer.class);
    private final int port;
    private final Javalin app;
    private final DatabaseSystem dbSystem;
    private final ConcurrentLinkedQueue<PageEvent> eventQueue;
    private Thread eventBroadcaster;
    private boolean running = false;
    
    // Map to store active WebSocket connections
    private final Map<WsContext, Boolean> wsConnections = new ConcurrentHashMap<>();

    /**
     * Creates a new web server.
     *
     * @param dbSystem The database system to visualize
     * @param port The port to run the server on
     */
    public WebServer(DatabaseSystem dbSystem, int port) {
        this.dbSystem = dbSystem;
        this.port = port;
        this.eventQueue = new ConcurrentLinkedQueue<>();
        
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        app = Javalin.create(config -> {
            // Configure CORS for local development
            config.plugins.enableCors(cors -> cors.add(CorsPluginConfig::anyHost));
            
            // Serve static files from the /public directory
            config.staticFiles.add(staticFiles -> {
                staticFiles.directory = "/public";
                staticFiles.location = Location.CLASSPATH;
                staticFiles.hostedPath = "/";
            });
            
            // Use Jackson for JSON serialization
            config.jsonMapper(new JavalinJackson(objectMapper));
        });
        
        // Configure API routes
        configureRoutes();
    }

    /**
     * Starts the web server.
     */
    public void start() {
        if (running) {
            return;
        }
        
        app.start(port);
        startEventBroadcaster();
        running = true;
        logger.info("Web server started on port {}", port);
    }

    /**
     * Stops the web server.
     */
    public void stop() {
        if (!running) {
            return;
        }
        
        stopEventBroadcaster();
        app.stop();
        running = false;
        logger.info("Web server stopped");
    }

    /**
     * Adds a page event to the event queue for broadcasting.
     *
     * @param event The page event to add
     */
    public void addPageEvent(PageEvent event) {
        eventQueue.add(event);
    }

    /**
     * Configures the API routes.
     */
    private void configureRoutes() {
        // Root endpoint - redirect to the React app
        app.get("/", ctx -> ctx.redirect("/index.html"));
        
        // Status endpoint
        app.get("/api/status", ctx -> {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "running");
            ctx.json(status);
        });
        
        // Get all tablespaces
        app.get("/api/tablespaces", ctx -> {
            List<TablespaceStatus> tablespaces = getTablespaceStatuses();
            ctx.json(tablespaces);
        });
        
        // Get a specific tablespace
        app.get("/api/tablespaces/{name}", ctx -> {
            String name = ctx.pathParam("name");
            TablespaceStatus tablespace = getTablespaceStatus(name);
            
            if (tablespace != null) {
                ctx.json(tablespace);
            } else {
                ctx.status(404).result("Tablespace not found");
            }
        });
        
        // Get all buffer pools
        app.get("/api/bufferpools", ctx -> {
            List<BufferPoolStatus> bufferPools = getBufferPoolStatuses();
            ctx.json(bufferPools);
        });
        
        // Get a specific buffer pool
        app.get("/api/bufferpools/{name}", ctx -> {
            String name = ctx.pathParam("name");
            BufferPoolStatus bufferPool = getBufferPoolStatus(name);
            
            if (bufferPool != null) {
                ctx.json(bufferPool);
            } else {
                ctx.status(404).result("Buffer pool not found");
            }
        });
        
        // WebSocket endpoint for real-time events
        app.ws("/api/events", ws -> {
            ws.onConnect(ctx -> {
                logger.info("New WebSocket connection: {}", ctx.getSessionId());
                wsConnections.put(ctx, true);
            });
            
            ws.onClose(ctx -> {
                logger.info("WebSocket connection closed: {}", ctx.getSessionId());
                wsConnections.remove(ctx);
            });
            
            ws.onError(ctx -> {
                logger.error("WebSocket error: {}", ctx.error().getMessage());
                wsConnections.remove(ctx);
            });
            
            ws.onMessage(ctx -> {
                // Handle client messages if needed
                logger.debug("Received WebSocket message: {}", ctx.message());
            });
        });
    }

    /**
     * Starts the event broadcaster thread.
     */
    private void startEventBroadcaster() {
        if (eventBroadcaster != null && eventBroadcaster.isAlive()) {
            return;
        }
        
        eventBroadcaster = new Thread(() -> {
            logger.info("Event broadcaster started");
            
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    PageEvent event = eventQueue.poll();
                    
                    if (event != null) {
                        // Broadcast the event to all connected WebSocket clients
                        broadcastEvent(event);
                    }
                    
                    Thread.sleep(100);  // Small delay to avoid CPU thrashing
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("Event broadcaster interrupted");
                    break;
                } catch (Exception e) {
                    logger.error("Error in event broadcaster", e);
                }
            }
            
            logger.info("Event broadcaster stopped");
        });
        
        eventBroadcaster.setDaemon(true);
        eventBroadcaster.start();
    }

    /**
     * Stops the event broadcaster thread.
     */
    private void stopEventBroadcaster() {
        if (eventBroadcaster != null && eventBroadcaster.isAlive()) {
            eventBroadcaster.interrupt();
            try {
                eventBroadcaster.join(1000);  // Wait for at most 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for event broadcaster to stop", e);
            }
        }
    }

    /**
     * Broadcasts an event to all connected WebSocket clients.
     *
     * @param event The event to broadcast
     */
    private void broadcastEvent(PageEvent event) {
        // Filter out closed sessions and send to all remaining ones
        wsConnections.keySet().stream()
            .filter(ctx -> ctx.session.isOpen())
            .forEach(session -> {
                try {
                    session.send(event);
                } catch (Exception e) {
                    logger.error("Error sending event to WebSocket client", e);
                }
            });
    }

    /**
     * Gets the status of all tablespaces.
     *
     * @return A list of tablespace statuses
     */
    private List<TablespaceStatus> getTablespaceStatuses() {
        List<TablespaceStatus> result = new ArrayList<>();
        StorageManager storageManager = dbSystem.getStorageManager();
        
        for (String name : dbSystem.getAllTablespaceNames()) {
            TablespaceStatus status = getTablespaceStatus(name);
            if (status != null) {
                result.add(status);
            }
        }
        
        return result;
    }

    /**
     * Gets the status of a specific tablespace.
     *
     * @param name The name of the tablespace
     * @return The tablespace status, or null if not found
     */
    private TablespaceStatus getTablespaceStatus(String name) {
        StorageManager storageManager = dbSystem.getStorageManager();
        
        try {
            // Use reflection to access the private tablespaces field
            Field tablespacesField = StorageManager.class.getDeclaredField("tablespaces");
            tablespacesField.setAccessible(true);
            Map<String, Tablespace> tablespaces = (Map<String, Tablespace>) tablespacesField.get(storageManager);
            
            Tablespace tablespace = tablespaces.get(name);
            
            if (tablespace == null) {
                return null;
            }
            
            String containerPath = tablespace.getStorageContainer().getContainerPath();
            int pageSize = tablespace.getPageSize();
            int totalPages;
            
            try {
                totalPages = tablespace.getTotalPages();
            } catch (Exception e) {
                totalPages = -1;
            }
            
            return new TablespaceStatus(name, containerPath, pageSize, totalPages);
        } catch (Exception e) {
            logger.error("Error getting tablespace status", e);
            return null;
        }
    }

    /**
     * Gets the status of all buffer pools.
     *
     * @return A list of buffer pool statuses
     */
    private List<BufferPoolStatus> getBufferPoolStatuses() {
        List<BufferPoolStatus> result = new ArrayList<>();
        
        try {
            // Use reflection to access the private bufferPoolManagers field
            Field bufferPoolManagersField = DatabaseSystem.class.getDeclaredField("bufferPoolManagers");
            bufferPoolManagersField.setAccessible(true);
            Map<String, BufferPoolManager> bufferPoolManagers = 
                    (Map<String, BufferPoolManager>) bufferPoolManagersField.get(dbSystem);
            
            for (Map.Entry<String, BufferPoolManager> entry : bufferPoolManagers.entrySet()) {
                String name = entry.getKey();
                BufferPoolManager bpm = entry.getValue();
                
                int size = bpm.getSize();
                int capacity = bpm.getCapacity();
                
                List<PageStatus> pages = getBufferPoolPages(bpm);
                
                result.add(new BufferPoolStatus(name, size, capacity, pages));
            }
        } catch (Exception e) {
            logger.error("Error getting buffer pool statuses", e);
        }
        
        return result;
    }

    /**
     * Gets the status of a specific buffer pool.
     *
     * @param name The name of the buffer pool
     * @return The buffer pool status, or null if not found
     */
    private BufferPoolStatus getBufferPoolStatus(String name) {
        try {
            // Use reflection to access the private bufferPoolManagers field
            Field bufferPoolManagersField = DatabaseSystem.class.getDeclaredField("bufferPoolManagers");
            bufferPoolManagersField.setAccessible(true);
            Map<String, BufferPoolManager> bufferPoolManagers = 
                    (Map<String, BufferPoolManager>) bufferPoolManagersField.get(dbSystem);
            
            BufferPoolManager bpm = bufferPoolManagers.get(name);
            
            if (bpm == null) {
                return null;
            }
            
            int size = bpm.getSize();
            int capacity = bpm.getCapacity();
            
            List<PageStatus> pages = getBufferPoolPages(bpm);
            
            return new BufferPoolStatus(name, size, capacity, pages);
        } catch (Exception e) {
            logger.error("Error getting buffer pool status", e);
            return null;
        }
    }

    /**
     * Gets the pages in a buffer pool.
     *
     * @param bpm The buffer pool manager
     * @return A list of page statuses
     */
    private List<PageStatus> getBufferPoolPages(BufferPoolManager bpm) {
        List<PageStatus> result = new ArrayList<>();
        
        try {
            // Use reflection to access the private pageTable field
            Field pageTableField = BufferPoolManager.class.getDeclaredField("pageTable");
            pageTableField.setAccessible(true);
            Map<?, ?> pageTable = (Map<?, ?>) pageTableField.get(bpm);
            
            // Skip this for now as it's more complex to extract and convert the page data
            // This would be implemented in a real system
        } catch (Exception e) {
            logger.error("Error getting buffer pool pages", e);
        }
        
        return result;
    }
} 