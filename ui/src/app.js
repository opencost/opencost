import React from 'react';
import ReactDOM from 'react-dom';
import { BrowserRouter as Router } from 'react-router-dom';

import Reports from './spa_index.jsx';

function ReportsPage() {
  return (
    <Router>
      <Reports path="/" />
    </Router>
  );
}

ReactDOM.render(
  <ReportsPage />,
  document.getElementById('app')
);
