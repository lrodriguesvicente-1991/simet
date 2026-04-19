import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { AuthProvider } from '@/contexts/AuthContext';
import { RoboProvider } from '@/contexts/RoboContext';
import ProtectedRoute from '@/components/ProtectedRoute';
import NivelGuard from '@/components/NivelGuard';
import Layout from '@/components/Layout';
import Login from '@/pages/Login';
import Observatorio from '@/pages/Observatorio';
import Comando from '@/pages/Comando';
import Usuarios from '@/pages/Usuarios';

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route element={<ProtectedRoute />}>
            <Route
              element={
                <RoboProvider>
                  <Layout />
                </RoboProvider>
              }
            >
              <Route index element={<Observatorio />} />
              <Route element={<NivelGuard nivelMaximo={2} />}>
                <Route path="comando" element={<Comando />} />
              </Route>
              <Route element={<NivelGuard nivelMaximo={0} />}>
                <Route path="usuarios" element={<Usuarios />} />
              </Route>
            </Route>
          </Route>
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  );
}
