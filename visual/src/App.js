import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import NavBar from './components/NavBar';
import Product from './pages/Product';
import Sales from './pages/Sales';
import Operations from './pages/Operations';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <NavBar />
        <main className="main-content">
          <Routes>
            {/* Default route redirects to Product */}
            <Route path="/" element={<Navigate to="/sales" replace />} />
            
            {/* Department routes */}
            <Route path="/product" element={<Product />} />
            <Route path="/sales" element={<Sales />} />
            <Route path="/operations" element={<Operations />} />
            
            {/* Catch-all route for 404 */}
            <Route path="*" element={<Navigate to="/sales" replace />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
