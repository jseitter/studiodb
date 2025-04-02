import { useEffect, useState } from 'react';
import { Card, Row, Col, ListGroup } from 'react-bootstrap';

// Define types for our data
interface TablespaceStatus {
  name: string;
  containerPath: string;
  pageSize: number;
  totalPages: number;
}

interface BufferPoolStatus {
  name: string;
  size: number;
  capacity: number;
}

const Dashboard = () => {
  const [tablespaces, setTablespaces] = useState<TablespaceStatus[]>([]);
  const [bufferPools, setBufferPools] = useState<BufferPoolStatus[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        setError(null);

        // Fetch tablespaces
        const tablespacesResponse = await fetch('/api/tablespaces');
        const tablespacesData = await tablespacesResponse.json();
        setTablespaces(tablespacesData);

        // Fetch buffer pools
        const bufferpoolsResponse = await fetch('/api/bufferpools');
        const bufferpoolsData = await bufferpoolsResponse.json();
        setBufferPools(bufferpoolsData);

        setIsLoading(false);
      } catch (err) {
        setError('Failed to load data. Is the database running with visualization enabled?');
        setIsLoading(false);
      }
    };

    fetchData();

    // Refresh data every 5 seconds
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div className="alert alert-danger">{error}</div>;
  }

  return (
    <div>
      <h1>StudioDB System Overview</h1>
      <p className="lead">
        Welcome to the StudioDB visualization interface. This dashboard provides real-time 
        information about the internal state of the database system.
      </p>

      <Row className="mt-4">
        <Col md={6}>
          <Card className="mb-4">
            <Card.Header>
              <h5>Tablespaces ({tablespaces.length})</h5>
            </Card.Header>
            <Card.Body>
              <ListGroup>
                {tablespaces.map((ts) => (
                  <ListGroup.Item key={ts.name} className="d-flex justify-content-between align-items-center">
                    <div>
                      <strong>{ts.name}</strong>
                      <div><small>{ts.containerPath}</small></div>
                    </div>
                    <div>
                      <span className="badge bg-info me-2">{ts.pageSize} bytes</span>
                      <span className="badge bg-primary">{ts.totalPages} pages</span>
                    </div>
                  </ListGroup.Item>
                ))}
              </ListGroup>
            </Card.Body>
          </Card>
        </Col>

        <Col md={6}>
          <Card className="mb-4">
            <Card.Header>
              <h5>Buffer Pools ({bufferPools.length})</h5>
            </Card.Header>
            <Card.Body>
              <ListGroup>
                {bufferPools.map((bp) => (
                  <ListGroup.Item key={bp.name} className="d-flex justify-content-between align-items-center">
                    <div>
                      <strong>{bp.name} Buffer Pool</strong>
                    </div>
                    <div>
                      <div className="progress" style={{ width: '200px' }}>
                        <div
                          className="progress-bar"
                          role="progressbar"
                          style={{ width: `${(bp.size / bp.capacity) * 100}%` }}
                          aria-valuenow={(bp.size / bp.capacity) * 100}
                          aria-valuemin={0}
                          aria-valuemax={100}
                        >
                          {bp.size}/{bp.capacity} pages
                        </div>
                      </div>
                    </div>
                  </ListGroup.Item>
                ))}
              </ListGroup>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      <Row>
        <Col md={12}>
          <Card>
            <Card.Header>
              <h5>Visualization Features</h5>
            </Card.Header>
            <Card.Body>
              <p>
                The StudioDB visualization interface allows you to observe the internal workings of the database system:
              </p>
              <ul>
                <li>View detailed information about <strong>tablespaces</strong> and their storage containers.</li>
                <li>Monitor <strong>buffer pools</strong> and the pages they contain, including page state (clean, dirty, pinned).</li>
                <li>Observe real-time <strong>page events</strong> like reads, writes, allocations, and evictions.</li>
              </ul>
              <p>
                Use the navigation menu to explore different aspects of the database system.
              </p>
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard; 