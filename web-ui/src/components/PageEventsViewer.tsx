import { useEffect, useState } from 'react';
import { Card, ListGroup, Badge } from 'react-bootstrap';

interface PageEvent {
  tablespaceName: string;
  pageId: number;
  eventType: string;
  timestamp: string;
  details: string;
}

const PageEventsViewer = () => {
  const [events, setEvents] = useState<PageEvent[]>([]);
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    // Connect to the WebSocket endpoint
    const socket = new WebSocket(`ws://${window.location.host}/api/events`);

    socket.onopen = () => {
      console.log('WebSocket connection established');
      setIsConnected(true);
    };

    socket.onclose = () => {
      console.log('WebSocket connection closed');
      setIsConnected(false);
    };

    socket.onerror = (error) => {
      console.error('WebSocket error:', error);
      setIsConnected(false);
    };

    socket.onmessage = (message) => {
      try {
        const event = JSON.parse(message.data);
        setEvents((prevEvents) => [event, ...prevEvents].slice(0, 50)); // Keep the 50 most recent events
      } catch (err) {
        console.error('Error parsing WebSocket message:', err);
      }
    };

    return () => {
      socket.close();
    };
  }, []);

  // Function to get the appropriate badge color for each event type
  const getEventBadgeVariant = (eventType: string): string => {
    switch (eventType) {
      case 'PAGE_READ':
        return 'primary';
      case 'PAGE_WRITE':
        return 'success';
      case 'PAGE_ALLOCATE':
        return 'info';
      case 'PAGE_PIN':
        return 'secondary';
      case 'PAGE_UNPIN':
        return 'light';
      case 'PAGE_DIRTY':
        return 'warning';
      case 'PAGE_EVICT':
        return 'danger';
      default:
        return 'dark';
    }
  };

  // Format timestamp
  const formatTimestamp = (timestamp: string): string => {
    const date = new Date(timestamp);
    return date.toLocaleTimeString();
  };

  return (
    <div>
      <h1>Page Events</h1>
      <p className="lead">
        Watch real-time page events as they occur in the buffer pool system.
      </p>

      <div className="mb-3">
        <strong>WebSocket Status: </strong>
        {isConnected ? (
          <Badge bg="success">Connected</Badge>
        ) : (
          <Badge bg="danger">Disconnected</Badge>
        )}
      </div>

      <Card>
        <Card.Header>
          <h5>Live Events Feed</h5>
        </Card.Header>
        <Card.Body className="p-0">
          {events.length === 0 ? (
            <div className="p-3 text-center">
              <p className="text-muted">No events received yet. Perform some database operations to see events.</p>
            </div>
          ) : (
            <ListGroup variant="flush">
              {events.map((event, index) => (
                <ListGroup.Item key={index} className="d-flex align-items-start">
                  <div className="me-3">
                    <Badge bg={getEventBadgeVariant(event.eventType)}>
                      {event.eventType.replace('PAGE_', '')}
                    </Badge>
                  </div>
                  <div className="flex-grow-1">
                    <div>
                      <strong>{event.tablespaceName}</strong> | Page #{event.pageId}
                    </div>
                    <div className="text-muted small">{event.details}</div>
                  </div>
                  <div className="small text-nowrap ms-2">
                    {formatTimestamp(event.timestamp)}
                  </div>
                </ListGroup.Item>
              ))}
            </ListGroup>
          )}
        </Card.Body>
      </Card>
    </div>
  );
};

export default PageEventsViewer; 