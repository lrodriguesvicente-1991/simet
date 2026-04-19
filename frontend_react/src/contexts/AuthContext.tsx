import { createContext, useCallback, useContext, useEffect, useState, type ReactNode } from 'react';
import { api, STORAGE_KEYS } from '@/lib/api';

interface LoginResponse {
  sucesso: boolean;
  token?: string;
  nivel?: number;
  usuario?: string;
  mensagem?: string;
}

interface AuthContextValue {
  token: string | null;
  usuario: string | null;
  nivel: number | null;
  loginEm: string | null;
  isAuthenticated: boolean;
  podeOperar: boolean;      // nivel <= 1
  podeAcompanhar: boolean;  // nivel <= 2
  isAdmin: boolean;         // nivel === 0
  login: (usuario: string, senha: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

function lerNivelLocal(): number | null {
  const raw = localStorage.getItem(STORAGE_KEYS.nivel);
  if (raw === null) return null;
  const n = parseInt(raw, 10);
  return Number.isFinite(n) ? n : null;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem(STORAGE_KEYS.token));
  const [usuario, setUsuario] = useState<string | null>(() => localStorage.getItem(STORAGE_KEYS.usuario));
  const [nivel, setNivel] = useState<number | null>(() => lerNivelLocal());
  const [loginEm, setLoginEm] = useState<string | null>(() => localStorage.getItem(STORAGE_KEYS.loginEm));

  // Revalida o perfil ao montar (caso o token esteja ativo mas o nivel tenha mudado no backend)
  useEffect(() => {
    if (!token) return;
    let cancelado = false;
    api
      .get<{ usuario: string; nivel: number }>('/me')
      .then((res) => {
        if (cancelado) return;
        setUsuario(res.data.usuario);
        setNivel(res.data.nivel);
        localStorage.setItem(STORAGE_KEYS.usuario, res.data.usuario);
        localStorage.setItem(STORAGE_KEYS.nivel, String(res.data.nivel));
      })
      .catch(() => {
        /* interceptor ja cuida de 401 */
      });
    return () => {
      cancelado = true;
    };
  }, [token]);

  const login = useCallback(async (user: string, senha: string) => {
    const res = await api.post<LoginResponse>('/login', { usuario: user, senha });
    if (!res.data.sucesso || !res.data.token) {
      throw new Error(res.data.mensagem || 'Credenciais inválidas');
    }
    const nivelRecebido = typeof res.data.nivel === 'number' ? res.data.nivel : 3;
    const agora = new Date().toISOString();
    localStorage.setItem(STORAGE_KEYS.token, res.data.token);
    localStorage.setItem(STORAGE_KEYS.usuario, user);
    localStorage.setItem(STORAGE_KEYS.nivel, String(nivelRecebido));
    localStorage.setItem(STORAGE_KEYS.loginEm, agora);
    setToken(res.data.token);
    setUsuario(user);
    setNivel(nivelRecebido);
    setLoginEm(agora);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(STORAGE_KEYS.token);
    localStorage.removeItem(STORAGE_KEYS.usuario);
    localStorage.removeItem(STORAGE_KEYS.nivel);
    localStorage.removeItem(STORAGE_KEYS.loginEm);
    setToken(null);
    setUsuario(null);
    setNivel(null);
    setLoginEm(null);
  }, []);

  const n = nivel ?? 3;

  return (
    <AuthContext.Provider
      value={{
        token,
        usuario,
        nivel,
        loginEm,
        isAuthenticated: !!token,
        podeOperar: n <= 1,
        podeAcompanhar: n <= 2,
        isAdmin: n === 0,
        login,
        logout,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth deve ser usado dentro de <AuthProvider>');
  return ctx;
}
