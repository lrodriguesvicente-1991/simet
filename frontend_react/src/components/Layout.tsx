import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { BarChart3, Cpu, LogOut, MapPin, Menu, X } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';

export default function Layout() {
  const [open, setOpen] = useState(true);
  const { logout, usuario } = useAuth();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  const linkClass = ({ isActive }: { isActive: boolean }) =>
    `w-full flex items-center gap-3 px-4 py-3 rounded-lg font-medium transition-colors ${
      isActive ? 'bg-accent text-primary' : 'text-muted-foreground hover:bg-muted'
    }`;

  return (
    <div className="flex h-screen bg-background font-sans text-foreground">
      <aside
        className={`${open ? 'w-64' : 'w-20'} bg-white border-r border-border transition-all duration-300 flex flex-col z-20`}
      >
        <div className="p-6 border-b border-border flex items-center gap-3">
          <div className="w-10 h-10 bg-primary rounded-lg flex items-center justify-center text-white shrink-0 shadow">
            <MapPin size={24} />
          </div>
          {open && <span className="font-bold text-lg text-primary">SIMET INCRA</span>}
        </div>

        <nav className="flex-1 p-4 space-y-2">
          <NavLink to="/" end className={linkClass}>
            <BarChart3 size={20} /> {open && 'Observatório de Terras'}
          </NavLink>
          <NavLink to="/comando" className={linkClass}>
            <Cpu size={20} /> {open && 'Central de Comando'}
          </NavLink>
        </nav>

        <div className="p-4 border-t border-border flex flex-col gap-2">
          {open && usuario && (
            <div className="px-2 py-1 text-xs text-muted-foreground">
              <div className="font-bold uppercase">Usuário</div>
              <div className="truncate">{usuario}</div>
            </div>
          )}
          <button
            onClick={handleLogout}
            className="w-full flex justify-center items-center gap-2 p-2 hover:bg-red-50 text-red-600 rounded-lg transition-colors"
          >
            <LogOut size={20} /> {open && <span className="text-sm font-bold">Sair</span>}
          </button>
          <button
            onClick={() => setOpen(!open)}
            className="w-full flex justify-center p-2 hover:bg-muted rounded-lg text-muted-foreground mt-2"
            aria-label={open ? 'Recolher menu' : 'Expandir menu'}
          >
            {open ? <X size={20} /> : <Menu size={20} />}
          </button>
        </div>
      </aside>

      <div className="flex-1 flex flex-col overflow-hidden">
        <Outlet />
      </div>
    </div>
  );
}
