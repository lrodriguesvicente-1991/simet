import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { BarChart3, Cpu, LogOut, Menu, UserCircle2, Users, X } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';

const ROTULOS_NIVEL = ['Administrador', 'Operador', 'Acompanhante', 'Visualizador'];

export default function Layout() {
  const [open, setOpen] = useState(true);
  const { logout, usuario, nivel, loginEm, podeAcompanhar, isAdmin } = useAuth();
  const navigate = useNavigate();

  const loginFormatado = loginEm
    ? new Date(loginEm).toLocaleString('pt-BR', {
        day: '2-digit',
        month: '2-digit',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
      })
    : null;

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
          <img src="/favicon.png" alt="SIMET INCRA" className="w-10 h-10 shrink-0 object-contain" />
          {open && <span className="font-bold text-lg text-primary">SIMET INCRA</span>}
        </div>

        <nav className="flex-1 p-4 space-y-2">
          <NavLink to="/" end className={linkClass}>
            <BarChart3 size={20} /> {open && 'Observatório de Terras'}
          </NavLink>
          {podeAcompanhar && (
            <NavLink to="/comando" className={linkClass}>
              <Cpu size={20} /> {open && 'Central de Comando'}
            </NavLink>
          )}
          {isAdmin && (
            <NavLink to="/usuarios" className={linkClass}>
              <Users size={20} /> {open && 'Gestão de Usuários'}
            </NavLink>
          )}
        </nav>

        <div className="p-4 border-t border-border flex flex-col gap-2">
          {open && usuario && (
            <div className="flex items-start gap-2 px-2 py-1 text-xs text-muted-foreground">
              <UserCircle2 size={32} className="shrink-0 text-primary" strokeWidth={1.5} />
              <div className="min-w-0 flex-1">
                <div className="font-bold uppercase">Usuário</div>
                <div className="truncate">{usuario}</div>
                {nivel !== null && (
                  <div className="text-[10px] text-primary font-bold mt-0.5">
                    {ROTULOS_NIVEL[nivel] ?? `Nível ${nivel}`}
                  </div>
                )}
                {loginFormatado && (
                  <div className="text-[10px] mt-1 leading-tight">
                    <span className="font-bold uppercase">Entrou em</span>
                    <div>{loginFormatado}</div>
                  </div>
                )}
              </div>
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
