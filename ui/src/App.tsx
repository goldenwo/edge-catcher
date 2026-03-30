import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './pages/Dashboard'
import DataSources from './pages/DataSources'
import Formalize from './pages/Formalize'
import Hypotheses from './pages/Hypotheses'
import Results from './pages/Results'
import Settings from './pages/Settings'

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/hypotheses" element={<Hypotheses />} />
          <Route path="/results" element={<Results />} />
          <Route path="/formalize" element={<Formalize />} />
          <Route path="/data-sources" element={<DataSources />} />
          <Route path="/settings" element={<Settings />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  )
}
