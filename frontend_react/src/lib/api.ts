import axios from 'axios';

export const STORAGE_KEYS = {
  token: 'simet_token',
  usuario: 'simet_usuario',
  nivel: 'simet_nivel',
  loginEm: 'simet_login_em',
} as const;

export const api = axios.create({
  baseURL: '/api',
  timeout: 30000,
});

api.interceptors.request.use((config) => {
  const token = localStorage.getItem(STORAGE_KEYS.token);
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem(STORAGE_KEYS.token);
      localStorage.removeItem(STORAGE_KEYS.usuario);
      localStorage.removeItem(STORAGE_KEYS.nivel);
      localStorage.removeItem(STORAGE_KEYS.loginEm);
      if (window.location.pathname !== '/login') {
        window.location.href = '/login';
      }
    }
    return Promise.reject(error);
  },
);

export function extrairMensagemErro(e: unknown, fallback: string): string {
  return (
    (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
    (e instanceof Error ? e.message : fallback)
  );
}
