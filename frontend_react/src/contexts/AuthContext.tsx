import { createContext, useCallback, useContext, useState, type ReactNode } from 'react';
import { api } from '@/lib/api';

interface LoginResponse {
  sucesso: boolean;
  token?: string;
  mensagem?: string;
}

interface AuthContextValue {
  token: string | null;
  usuario: string | null;
  isAuthenticated: boolean;
  login: (usuario: string, senha: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem('simet_token'));
  const [usuario, setUsuario] = useState<string | null>(() => localStorage.getItem('simet_usuario'));

  const login = useCallback(async (user: string, senha: string) => {
    const res = await api.post<LoginResponse>('/login', { usuario: user, senha });
    if (!res.data.sucesso || !res.data.token) {
      throw new Error(res.data.mensagem || 'Credenciais inválidas');
    }
    localStorage.setItem('simet_token', res.data.token);
    localStorage.setItem('simet_usuario', user);
    setToken(res.data.token);
    setUsuario(user);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem('simet_token');
    localStorage.removeItem('simet_usuario');
    setToken(null);
    setUsuario(null);
  }, []);

  return (
    <AuthContext.Provider
      value={{ token, usuario, isAuthenticated: !!token, login, logout }}
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
