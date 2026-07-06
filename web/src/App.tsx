import React from 'react';
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom';
import { Activity, FileText, LayoutList, Workflow } from 'lucide-react';
import Runs from './pages/Runs';
import RunDetail from './pages/RunDetail';
import Materializations from './pages/Materializations';
import CurrentOutputs from './pages/CurrentOutputs';
import OutputDetail from './pages/OutputDetail';
import Pipelines from './pages/Pipelines';
import OuroborosLogo from './components/OuroborosLogo';

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="layout">
      <nav className="sidebar">
        <div className="sidebar-logo">
          <OuroborosLogo size={26} />
          Rubedo
        </div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
          <NavLink to="/" end className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Activity size={18} /> Runs
          </NavLink>
          <NavLink to="/pipelines" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <Workflow size={18} /> Pipelines
          </NavLink>
          <NavLink to="/materializations" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <FileText size={18} /> Materializations
          </NavLink>
          <NavLink to="/coordinates" className={({ isActive }) => `nav-link ${isActive ? 'active' : ''}`}>
            <LayoutList size={18} /> Current Outputs
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
          <Route path="/" element={<Runs />} />
          <Route path="/runs" element={<Runs />} />
          <Route path="/runs/:runId" element={<RunDetail />} />
          <Route path="/materializations" element={<Materializations />} />
          <Route path="/coordinates" element={<CurrentOutputs />} />
          <Route path="/pipelines" element={<Pipelines />} />
          <Route path="/objects/:address" element={<OutputDetail />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}

export default App;
