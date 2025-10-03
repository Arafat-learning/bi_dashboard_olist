import React from 'react';
import { Link, useLocation } from 'react-router-dom';

const NavBar = () => {
  const location = useLocation();

  const navItems = [
    { path: '/product', label: 'Order', color: '#8884d8' },
    { path: '/sales', label: 'Sales', color: '#82ca9d' },
    { path: '/operations', label: 'Operations', color: '#ffc658' }
  ];

  const navStyle = {
    backgroundColor: '#ffffff',
    borderBottom: '2px solid #f0f0f0',
    padding: '0 2rem',
    boxShadow: '0 2px 4px rgba(0,0,0,0.1)',
    position: 'sticky',
    top: 0,
    zIndex: 1000
  };

  const containerStyle = {
    display: 'flex',
    alignItems: 'center',
    maxWidth: '1200px',
    margin: '0 auto',
    height: '60px'
  };

  const logoStyle = {
    fontSize: '24px',
    fontWeight: '700',
    color: '#2c3e50',
    marginRight: '3rem',
    textDecoration: 'none'
  };

  const navListStyle = {
    display: 'flex',
    listStyle: 'none',
    margin: 0,
    padding: 0,
    gap: '2rem'
  };

  const getLinkStyle = (isActive, color) => ({
    textDecoration: 'none',
    fontSize: '16px',
    fontWeight: '500',
    color: isActive ? color : '#666',
    padding: '0.5rem 1rem',
    borderRadius: '6px',
    transition: 'all 0.2s ease',
    backgroundColor: isActive ? `${color}15` : 'transparent',
    border: isActive ? `2px solid ${color}` : '2px solid transparent'
  });

  const getLinkHoverStyle = (color) => ({
    color: color,
    backgroundColor: `${color}08`,
    transform: 'translateY(-1px)'
  });

  return (
    <nav style={navStyle}>
      <div style={containerStyle}>
        <Link to="/" style={logoStyle}>
          📊 Analytics Dashboard
        </Link>
        
        <ul style={navListStyle}>
          {navItems.map(({ path, label, color }) => {
            const isActive = location.pathname === path;
            return (
              <li key={path}>
                <Link
                  to={path}
                  style={getLinkStyle(isActive, color)}
                  onMouseEnter={(e) => {
                    if (!isActive) {
                      Object.assign(e.target.style, getLinkHoverStyle(color));
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!isActive) {
                      e.target.style.color = '#666';
                      e.target.style.backgroundColor = 'transparent';
                      e.target.style.transform = 'translateY(0)';
                    }
                  }}
                >
                  {label}
                </Link>
              </li>
            );
          })}
        </ul>
      </div>
    </nav>
  );
};

export default NavBar;
