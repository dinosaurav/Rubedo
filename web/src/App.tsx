import React from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { LayoutDashboard, Activity, Database, FileText, LayoutList } from 'lucide-react';
import Dashboard from './pages/Dashboard';
import Runs from './pages/Runs';
import RunDetail from './pages/RunDetail';
import Materializations from './pages/Materializations';
import CurrentOutputs from './pages/CurrentOutputs';
import SelectionBuilder from './pages/SelectionBuilder';
import OutputDetail from './pages/OutputDetail';
import RunDiff from './pages/RunDiff';
import Pipelines from './pages/Pipelines';

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="layout">
      <nav className="sidebar">
        <div className="sidebar-logo">
          <Database color="var(--accent-primary)" />
          BatchBrain
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
          <NavLink to="/" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <LayoutDashboard size={20} /> Dashboard
          </NavLink>
          <NavLink to="/runs" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Activity size={20} /> Runs
          </NavLink>
          <NavLink to="/materializations" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <FileText size={20} /> Materializations
          </NavLink>
          <NavLink to="/coordinates" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <LayoutList size={20} /> Current Outputs
          </NavLink>
          <NavLink to="/select" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Database size={20} /> Selection UI
          </NavLink>
          <NavLink to="/diff" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Activity size={20} /> Compare Runs
          </NavLink>
          <NavLink to="/pipelines" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Activity size={20} /> Pipelines
          </NavLink>
        </div>
      </nav>
      <main className="main-content">
        {children}
      </main>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/runs" element={<Runs />} />
          <Route path="/runs/:runId" element={<RunDetail />} />
          <Route path="/materializations" element={<Materializations />} />
          <Route path="/coordinates" element={<CurrentOutputs />} />
          <Route path="/select" element={<SelectionBuilder />} />
          <Route path="/diff" element={<RunDiff />} />
          <Route path="/pipelines" element={<Pipelines />} />
          <Route path="/objects/:address" element={<OutputDetail />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
