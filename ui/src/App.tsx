import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/Layout'
import { PipelineProvider } from './components/PipelineStatus'
import Dashboard from './pages/Dashboard'
import DataSources from './pages/DataSources'
import Hypotheses from './pages/Hypotheses'
import Analyze from './pages/Analyze'
import Strategize from './pages/Strategize'
import Backtest from './pages/Backtest'
import Settings from './pages/Settings'

export default function App() {
  return (
    <BrowserRouter>
      <PipelineProvider>
        <Layout>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/data-sources" element={<DataSources />} />
            <Route path="/hypotheses" element={<Hypotheses />} />
            <Route path="/analyze" element={<Analyze />} />
            <Route path="/strategize" element={<Strategize />} />
            <Route path="/backtest" element={<Backtest />} />
            <Route path="/settings" element={<Settings />} />
            {/* Redirects for old routes */}
            <Route path="/results" element={<Navigate to="/analyze" replace />} />
            <Route path="/formalize" element={<Navigate to="/hypotheses?tab=create" replace />} />
            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </Layout>
      </PipelineProvider>
    </BrowserRouter>
  )
}
