import { useState, type FormEvent } from 'react';
import { Navigate, useNavigate } from 'react-router-dom';
import { Eye, EyeOff, Lock, Loader2, User } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';

export default function Login() {
  const { login, isAuthenticated } = useAuth();
  const navigate = useNavigate();
  const [usuario, setUsuario] = useState('');
  const [senha, setSenha] = useState('');
  const [mostrarSenha, setMostrarSenha] = useState(false);
  const [erro, setErro] = useState('');
  const [loading, setLoading] = useState(false);

  if (isAuthenticated) return <Navigate to="/" replace />;

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setErro('');
    setLoading(true);
    try {
      await login(usuario, senha);
      navigate('/', { replace: true });
    } catch (err) {
      setErro(err instanceof Error ? err.message : 'Falha ao autenticar');
    } finally {
      setLoading(false);
    }
  };

  const inputClass =
    'w-full border border-border rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-primary';

  return (
    <div className="flex min-h-screen bg-background items-center justify-center p-4">
      <div className="w-full max-w-md bg-white rounded-2xl shadow-xl border border-border p-8">
        <div className="flex flex-col items-center mb-8">
          <img src="/favicon.png" alt="INCRA" className="h-20 mb-3 object-contain" />
          <h1 className="text-2xl font-bold text-foreground">SIMET</h1>
          <p className="text-muted-foreground text-sm text-center mt-1">
            Plataforma Analítica de Valores Fundiários
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          {erro && (
            <div className="bg-red-50 text-red-600 p-3 rounded-lg text-sm text-center border border-red-100">
              {erro}
            </div>
          )}

          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Usuário
            </label>
            <div className="relative">
              <User size={18} className="absolute top-2.5 left-3 text-muted-foreground" />
              <input
                type="text"
                autoComplete="username"
                value={usuario}
                onChange={(e) => setUsuario(e.target.value)}
                className={`${inputClass} pl-10`}
                required
              />
            </div>
          </div>

          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Senha
            </label>
            <div className="relative">
              <Lock size={18} className="absolute top-2.5 left-3 text-muted-foreground" />
              <input
                type={mostrarSenha ? 'text' : 'password'}
                autoComplete="current-password"
                value={senha}
                onChange={(e) => setSenha(e.target.value)}
                className={`${inputClass} pl-10 pr-10`}
                required
              />
              <button
                type="button"
                onClick={() => setMostrarSenha((v) => !v)}
                aria-label={mostrarSenha ? 'Ocultar senha' : 'Exibir senha'}
                className="absolute top-2.5 right-3 text-muted-foreground hover:text-foreground transition"
              >
                {mostrarSenha ? <EyeOff size={18} /> : <Eye size={18} />}
              </button>
            </div>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full bg-primary hover:bg-primary/90 text-white font-bold py-3 px-4 rounded-lg flex items-center justify-center gap-2 disabled:opacity-60 disabled:cursor-not-allowed transition"
          >
            {loading && <Loader2 size={18} className="animate-spin" />}
            {loading ? 'Autenticando...' : 'Entrar no Sistema'}
          </button>
        </form>

        <p className="text-xs text-muted-foreground text-center mt-6">
          Acesso restrito — uso exclusivo INCRA
        </p>
      </div>
    </div>
  );
}
