import React, { useState, useEffect } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

function Sales() {
  // State to store our data
  const [salesData, setSalesData] = useState([]);
  const [loading, setLoading] = useState(true);

  // useEffect runs when component loads
  useEffect(() => {
    // Fetch data from your pipeline's output JSON file
    fetch('/kpis/sales.json')
      .then(response => response.json())
      .then(data => {
        setSalesData(data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Error loading sales data:', error);
        setLoading(false);
      });
  }, []); // Empty array means this runs once when component mounts

  // Show loading message while data is being fetched
  if (loading) {
    return <div>Loading sales data...</div>;
  }

  return (
    <div style={{ padding: '20px' }}>
      <h1>Sales Dashboard</h1>
      
      <div style={{ marginTop: '30px' }}>
        <h2>Monthly Sales Trend</h2>
        <ResponsiveContainer width="100%" height={400}>
          <LineChart data={salesData}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey= "month" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="sales" stroke="#82ca9d" strokeWidth={2} />
            <Line type="monotone" dataKey="growth" stroke="#8884d8" strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

export default Sales;
