import { Navigate, Outlet } from 'react-router-dom';
import { useAuth } from '@/contexts/AuthContext';

interface Props {
  nivelMaximo: number;
  redirecionaPara?: string;
}

/**
 * Bloqueia sub-rotas para usuarios com nivel acima do permitido.
 * Lembre-se: 0 = Admin (mais poder), 3 = Visualizador (menos poder).
 */
export default function NivelGuard({ nivelMaximo, redirecionaPara = '/' }: Props) {
  const { nivel, isAuthenticated } = useAuth();
  if (!isAuthenticated) return <Navigate to="/login" replace />;
  const n = nivel ?? 3;
  if (n > nivelMaximo) return <Navigate to={redirecionaPara} replace />;
  return <Outlet />;
}
