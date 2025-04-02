import { useEffect, useState } from 'react';
import { Card, Row, Col, Table } from 'react-bootstrap';

interface TablespaceStatus {
  name: string;
  containerPath: string;
  pageSize: number;
  totalPages: number;
}

const TablespaceViewer = () => {
  const [tablespaces, setTablespaces] = useState<TablespaceStatus[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setIsLoading(true);
        const response = await fetch('/api/tablespaces');
        const data = await response.json();
        setTablespaces(data);
        setIsLoading(false);
      } catch (err) {
        setError('Failed to load tablespaces');
        setIsLoading(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, []);

  if (isLoading) {
    return <div>Loading tablespaces...</div>;
  }

  if (error) {
    return <div className="alert alert-danger">{error}</div>;
  }

  return (
    <div>
      <h1>Tablespaces</h1>
      <p className="lead">
        Tablespaces are containers that store database objects (tables, indexes) in files on disk.
      </p>

      <Row>
        <Col>
          <Card>
            <Card.Header>
              <h5>Tablespace Details</h5>
            </Card.Header>
            <Card.Body>
              {tablespaces.length === 0 ? (
                <p>No tablespaces found.</p>
              ) : (
                <Table striped bordered hover>
                  <thead>
                    <tr>
                      <th>Name</th>
                      <th>Container Path</th>
                      <th>Page Size</th>
                      <th>Total Pages</th>
                      <th>Total Size</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tablespaces.map((ts) => (
                      <tr key={ts.name}>
                        <td>{ts.name}</td>
                        <td><code>{ts.containerPath}</code></td>
                        <td>{ts.pageSize} bytes</td>
                        <td>{ts.totalPages}</td>
                        <td>{((ts.pageSize * ts.totalPages) / (1024 * 1024)).toFixed(2)} MB</td>
                      </tr>
                    ))}
                  </tbody>
                </Table>
              )}
            </Card.Body>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default TablespaceViewer; 