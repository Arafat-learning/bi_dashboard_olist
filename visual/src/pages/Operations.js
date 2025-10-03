import React, { useState, useEffect } from 'react';

function Operations() {
  const [kpiData, setKpiData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch KPI data from your pipeline's output JSON file
    fetch('/kpis/operations.json')
      .then(response => response.json())
      .then(data => {
        setKpiData(data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Error loading operations data:', error);
        setLoading(false);
      });
  }, []);

  if (loading) {
    return <div style={{ padding: '20px' }}>Loading operations data...</div>;
  }

  if (!kpiData) {
    return <div style={{ padding: '20px' }}>Failed to load operations data</div>;
  }

  return (
    <div style={{ padding: '20px' }}>
      <h1 style={{ 
        fontSize: '2rem', 
        fontWeight: '600', 
        marginBottom: '2rem',
        color: '#2c3e50'
      }}>
        Operations Dashboard
      </h1>
      
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
        gap: '20px',
        marginTop: '30px'
      }}>
        
        {/* Total Orders Delivered KPI Card */}
        <div style={{
          background: 'white',
          borderRadius: '12px',
          padding: '30px',
          boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
          border: '2px solid #ffc658',
          transition: 'transform 0.2s',
          cursor: 'default'
        }}>
          <div style={{
            fontSize: '14px',
            color: '#666',
            textTransform: 'uppercase',
            letterSpacing: '1px',
            marginBottom: '10px',
            fontWeight: '600'
          }}>
            Total Orders Delivered
          </div>
          <div style={{
            fontSize: '3rem',
            fontWeight: '700',
            color: '#ffc658',
            marginBottom: '10px'
          }}>
            {kpiData.total_deliveries?.toLocaleString() || 'N/A'}
          </div>
          <div style={{
            fontSize: '14px',
            color: '#888'
          }}>
            📦 Completed deliveries
          </div>
        </div>

        {/* Average Delivery Time KPI Card */}
        <div style={{
          background: 'white',
          borderRadius: '12px',
          padding: '30px',
          boxShadow: '0 4px 6px rgba(0,0,0,0.1)',
          border: '2px solid #8884d8',
          transition: 'transform 0.2s',
          cursor: 'default'
        }}>
          <div style={{
            fontSize: '14px',
            color: '#666',
            textTransform: 'uppercase',
            letterSpacing: '1px',
            marginBottom: '10px',
            fontWeight: '600'
          }}>
            Average Delivery Time
          </div>
          <div style={{
            fontSize: '3rem',
            fontWeight: '700',
            color: '#8884d8',
            marginBottom: '10px'
          }}>
            {kpiData.delivery_time?.toFixed(1) || 'N/A'}
          </div>
          <div style={{
            fontSize: '14px',
            color: '#888'
          }}>
            ⏱️ Days to deliver
          </div>
        </div>

      </div>
    </div>
  );
}

export default Operations;
