import React, { useState, useEffect } from 'react';

function Product() {
  const [kpiData, setKpiData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch KPI data from your pipeline's output JSON file
    fetch('/kpis/order_stat.json')
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
        Order Statistics Dashboard
      </h1>
      
      <div style={{ 
        display: 'grid', 
        gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))',
        gap: '20px',
        marginTop: '30px'
      }}>
        
        {/* Average Order Price KPI Card */}
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
            Average Order Price 
          </div>
          <div style={{
            fontSize: '3rem',
            fontWeight: '700',
            color: '#ffc658',
            marginBottom: '10px'
          }}>
            {kpiData.order_spending?.toLocaleString() || 'N/A'}
          </div>
          <div style={{
            fontSize: '14px',
            color: '#888'
          }}>
            💷 Brazillian Reals 
          </div>
        </div>

        {/* Average Item Count KPI Card */}
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
           Average Item Count 
          </div>
          <div style={{
            fontSize: '3rem',
            fontWeight: '700',
            color: '#8884d8',
            marginBottom: '10px'
          }}>
            {kpiData.avg_item_count?.toFixed(1) || 'N/A'}
          </div>
          <div style={{
            fontSize: '14px',
            color: '#888'
          }}>
            🏷️ Items per Order 
          </div>
        </div>

      </div>
    </div>
  );
}

export default Product;
