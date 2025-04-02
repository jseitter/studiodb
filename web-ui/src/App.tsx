import { useEffect, useState } from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import { Container, Nav, Navbar } from 'react-bootstrap';

import 'bootstrap/dist/css/bootstrap.min.css';
import './App.css';

import Dashboard from './components/Dashboard';
import TablespaceViewer from './components/TablespaceViewer';
import BufferPoolViewer from './components/BufferPoolViewer';
import PageEventsViewer from './components/PageEventsViewer';

function App() {
  const [isConnected, setIsConnected] = useState(false);

  useEffect(() => {
    // Check if the backend is available
    fetch('/api/status')
      .then(response => response.json())
      .then(data => {
        if (data.status === 'running') {
          setIsConnected(true);
        }
      })
      .catch(() => {
        setIsConnected(false);
      });
  }, []);

  return (
    <Router>
      <div className="App">
        <Navbar bg="dark" variant="dark" expand="lg">
          <Container>
            <Navbar.Brand as={Link} to="/">StudioDB Visualization</Navbar.Brand>
            <Navbar.Toggle aria-controls="basic-navbar-nav" />
            <Navbar.Collapse id="basic-navbar-nav">
              <Nav className="me-auto">
                <Nav.Link as={Link} to="/">Dashboard</Nav.Link>
                <Nav.Link as={Link} to="/tablespaces">Tablespaces</Nav.Link>
                <Nav.Link as={Link} to="/bufferpools">Buffer Pools</Nav.Link>
                <Nav.Link as={Link} to="/events">Page Events</Nav.Link>
              </Nav>
              <Nav>
                <Nav.Link className={isConnected ? 'text-success' : 'text-danger'}>
                  {isConnected ? 'Connected' : 'Disconnected'}
                </Nav.Link>
              </Nav>
            </Navbar.Collapse>
          </Container>
        </Navbar>

        <Container className="mt-3">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/tablespaces" element={<TablespaceViewer />} />
            <Route path="/bufferpools" element={<BufferPoolViewer />} />
            <Route path="/events" element={<PageEventsViewer />} />
          </Routes>
        </Container>
      </div>
    </Router>
  );
}

export default App; 