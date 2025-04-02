import { useEffect, useState } from 'react';
import { Card, Row, Col, ProgressBar } from 'react-bootstrap';

interface PageStatus {
  tablespaceName: string;
  pageId: number;
  dirty: boolean;
  pinCount: number;
  modified: boolean;
}

interface BufferPoolStatus {
  name: string;
  size: number;
  capacity: number;
  pages: PageStatus[];
}

const BufferPoolViewer = () => {
  const [bufferPools, setBufferPools] = useState<BufferPoolStatus[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        const response = await fetch('/api/bufferpools');
        const data = await response.json();
        setBufferPools(data);
        setIsLoading(false);
      } catch (err) {
        setError('Failed to load buffer pools');
        setIsLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  if (isLoading) {
    return <div>Loading buffer pools...</div>;
  }

  if (error) {
    return <div className="alert alert-danger">{error}</div>;
  }

  return (
    <div>
      <h1>Buffer Pools</h1>
      <p className="lead">
        Buffer pools cache pages in memory to reduce disk I/O. 
        They implement the buffer replacement policy and manage page pinning.
      </p>

      {bufferPools.map((bp) => (
        <Card key={bp.name} className="mb-4">
          <Card.Header>
            <h5>{bp.name} Buffer Pool</h5>
          </Card.Header>
          <Card.Body>
            <Row className="mb-3">
              <Col md={6}>
                <strong>Capacity:</strong> {bp.capacity} pages
              </Col>
              <Col md={6}>
                <strong>Current Size:</strong> {bp.size} pages
              </Col>
            </Row>
            <Row className="mb-3">
              <Col md={12}>
                <strong>Utilization:</strong>
                <ProgressBar 
                  now={(bp.size / bp.capacity) * 100}
                  label={`${bp.size}/${bp.capacity} pages (${((bp.size / bp.capacity) * 100).toFixed(1)}%)`}
                  variant={getProgressVariant(bp.size, bp.capacity)}
                  className="mt-2"
                />
              </Col>
            </Row>
            <Row>
              <Col>
                <strong>Page States:</strong>
                <div className="d-flex mt-2">
                  <div className="me-3">
                    <span className="badge bg-success me-1">&nbsp;</span> Clean
                  </div>
                  <div className="me-3">
                    <span className="badge bg-warning me-1">&nbsp;</span> Dirty
                  </div>
                  <div>
                    <span className="badge bg-info me-1">&nbsp;</span> Pinned
                  </div>
                </div>
              </Col>
            </Row>
          </Card.Body>
        </Card>
      ))}
    </div>
  );
};

// Helper function to get the right variant for the progress bar
const getProgressVariant = (size: number, capacity: number): string => {
  const percentage = (size / capacity) * 100;
  if (percentage < 50) return 'success';
  if (percentage < 75) return 'info';
  if (percentage < 90) return 'warning';
  return 'danger';
};

export default BufferPoolViewer; 