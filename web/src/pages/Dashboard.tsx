import React, { useEffect, useState } from 'react';
import { fetchRuns, fetchMaterializations, fetchCurrentOutputs } from '../api';

export default function Dashboard() {
  const [stats, setStats] = useState({
    runs: 0,
    mats: 0,
    current: 0
  });

  useEffect(() => {
    Promise.all([fetchRuns(), fetchMaterializations(), fetchCurrentOutputs()])
      .then(([runs, mats, current]) => {
        setStats({
          runs: runs.length,
          mats: mats.total,
          current: current.length
        });
      });
  }, []);

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Dashboard</h1>
      </div>
      <div className="stats-grid">
        <div className="card stat-card">
          <div className="stat-label">Total Runs</div>
          <div className="stat-value">{stats.runs}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Total Materializations</div>
          <div className="stat-value">{stats.mats}</div>
        </div>
        <div className="card stat-card">
          <div className="stat-label">Current Outputs</div>
          <div className="stat-value">{stats.current}</div>
        </div>
      </div>
    </div>
  );
}
