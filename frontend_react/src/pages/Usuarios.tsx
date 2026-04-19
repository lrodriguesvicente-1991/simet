import { useEffect, useState } from 'react';
import { KeyRound, Plus, ShieldCheck, Trash2, UserCheck, UserMinus, UserPlus } from 'lucide-react';
import { api, extrairMensagemErro } from '@/lib/api';
import { useAuth } from '@/contexts/AuthContext';

interface Usuario {
  id: number;
  username: string;
  nivel: number;
  ativo: boolean;
  criado_em: string | null;
}

const NIVEIS: { valor: number; titulo: string; desc: string }[] = [
  { valor: 0, titulo: 'Administrador', desc: 'Acesso total + gestão de usuários' },
  { valor: 1, titulo: 'Operador', desc: 'Executa robôs, exporta relatórios, sincroniza base' },
  { valor: 2, titulo: 'Acompanhante', desc: 'Vê a operação dos robôs e o observatório' },
  { valor: 3, titulo: 'Visualizador', desc: 'Somente observatório de terras' },
];

function rotuloNivel(n: number): string {
  return NIVEIS.find((x) => x.valor === n)?.titulo ?? `Nível ${n}`;
}

export default function Usuarios() {
  const { usuario: usuarioAtual } = useAuth();
  const [lista, setLista] = useState<Usuario[]>([]);
  const [carregando, setCarregando] = useState(false);
  const [erro, setErro] = useState<string | null>(null);

  const [novoUser, setNovoUser] = useState('');
  const [novaSenha, setNovaSenha] = useState('');
  const [novoNivel, setNovoNivel] = useState(3);
  const [criando, setCriando] = useState(false);

  const [resetId, setResetId] = useState<number | null>(null);
  const [resetSenha, setResetSenha] = useState('');

  const carregar = async () => {
    try {
      setCarregando(true);
      const res = await api.get<{ sucesso: boolean; usuarios: Usuario[] }>('/usuarios');
      setLista(res.data.usuarios || []);
      setErro(null);
    } catch (e) {
      setErro(e instanceof Error ? e.message : 'Falha ao carregar usuários');
    } finally {
      setCarregando(false);
    }
  };

  useEffect(() => {
    carregar();
  }, []);

  const handleCriar = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!novoUser.trim() || novaSenha.length < 4) {
      setErro('Informe um usuário válido e senha de pelo menos 4 caracteres.');
      return;
    }
    try {
      setCriando(true);
      await api.post('/usuarios', { usuario: novoUser.trim(), senha: novaSenha, nivel: novoNivel });
      setNovoUser('');
      setNovaSenha('');
      setNovoNivel(3);
      await carregar();
    } catch (e) {
      setErro(extrairMensagemErro(e, 'Falha ao criar usuário'));
    } finally {
      setCriando(false);
    }
  };

  const atualizar = async (u: Usuario, patch: { nivel?: number; ativo?: boolean }) => {
    try {
      await api.patch(`/usuarios/${u.id}`, patch);
      await carregar();
    } catch (e) {
      setErro(extrairMensagemErro(e, 'Falha ao atualizar'));
    }
  };

  const excluir = async (u: Usuario) => {
    if (!confirm(`Excluir o usuário "${u.username}"? Esta ação é irreversível.`)) return;
    try {
      await api.delete(`/usuarios/${u.id}`);
      await carregar();
    } catch (e) {
      setErro(extrairMensagemErro(e, 'Falha ao excluir'));
    }
  };

  const confirmarReset = async () => {
    if (resetId === null || resetSenha.length < 4) {
      setErro('Nova senha precisa ter pelo menos 4 caracteres.');
      return;
    }
    try {
      await api.post(`/usuarios/${resetId}/reset-senha`, { nova_senha: resetSenha });
      setResetId(null);
      setResetSenha('');
      setErro(null);
    } catch (e) {
      setErro(extrairMensagemErro(e, 'Falha ao redefinir senha'));
    }
  };

  return (
    <>
      <header className="bg-white border-b border-border px-8 py-6 shrink-0">
        <h1 className="text-2xl font-bold">Gestão de Usuários</h1>
        <p className="text-muted-foreground text-sm">
          Cadastro, níveis de permissão e redefinição de senhas
        </p>
      </header>

      <div className="flex-1 overflow-auto p-8 space-y-6">
        {erro && (
          <div className="bg-red-50 border border-red-200 text-red-800 p-3 rounded-lg text-sm">
            {erro}
          </div>
        )}

        <section className="bg-white border border-border rounded-xl shadow-sm p-6">
          <div className="flex items-center gap-2 mb-4">
            <UserPlus size={18} className="text-primary" />
            <h2 className="font-bold">Novo usuário</h2>
          </div>
          <form onSubmit={handleCriar} className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <input
              type="text"
              placeholder="Nome de usuário"
              value={novoUser}
              onChange={(e) => setNovoUser(e.target.value)}
              className="border border-border rounded-md px-3 py-2 text-sm"
              autoComplete="off"
            />
            <input
              type="password"
              placeholder="Senha inicial"
              value={novaSenha}
              onChange={(e) => setNovaSenha(e.target.value)}
              className="border border-border rounded-md px-3 py-2 text-sm"
              autoComplete="new-password"
            />
            <select
              value={novoNivel}
              onChange={(e) => setNovoNivel(parseInt(e.target.value, 10))}
              className="border border-border rounded-md px-3 py-2 text-sm"
            >
              {NIVEIS.map((n) => (
                <option key={n.valor} value={n.valor}>
                  {n.titulo} — {n.desc}
                </option>
              ))}
            </select>
            <button
              type="submit"
              disabled={criando}
              className="bg-primary text-white rounded-md font-bold text-sm py-2 px-4 flex justify-center items-center gap-2 hover:bg-primary/90 disabled:opacity-60"
            >
              <Plus size={14} /> Cadastrar
            </button>
          </form>
          <p className="text-xs text-muted-foreground mt-2">
            Os 4 níveis estão descritos na lista. O usuário poderá trocar a senha depois.
          </p>
        </section>

        <section className="bg-white border border-border rounded-xl shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-border flex items-center gap-2">
            <ShieldCheck size={18} className="text-primary" />
            <h2 className="font-bold">Usuários cadastrados</h2>
            <span className="text-muted-foreground text-sm">({lista.length})</span>
          </div>
          {carregando ? (
            <div className="p-6 text-sm text-muted-foreground animate-pulse">Carregando...</div>
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-muted text-xs uppercase text-muted-foreground">
                <tr>
                  <th className="px-4 py-3 text-left">Usuário</th>
                  <th className="px-4 py-3 text-left">Nível</th>
                  <th className="px-4 py-3 text-left">Estado</th>
                  <th className="px-4 py-3 text-left">Criado em</th>
                  <th className="px-4 py-3 text-right">Ações</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {lista.map((u) => {
                  const ehVoce = u.username === usuarioAtual;
                  return (
                    <tr key={u.id} className="hover:bg-muted/40">
                      <td className="px-4 py-3 font-medium">
                        {u.username}
                        {ehVoce && <span className="ml-2 text-[10px] text-primary font-bold">(você)</span>}
                      </td>
                      <td className="px-4 py-3">
                        <select
                          value={u.nivel}
                          onChange={(e) => atualizar(u, { nivel: parseInt(e.target.value, 10) })}
                          className="border border-border rounded px-2 py-1 text-sm bg-white"
                          disabled={ehVoce && u.nivel === 0}
                          title={rotuloNivel(u.nivel)}
                        >
                          {NIVEIS.map((n) => (
                            <option key={n.valor} value={n.valor}>
                              {n.titulo}
                            </option>
                          ))}
                        </select>
                      </td>
                      <td className="px-4 py-3">
                        <span
                          className={`text-xs font-bold px-2 py-1 rounded ${
                            u.ativo
                              ? 'bg-emerald-100 text-emerald-700'
                              : 'bg-slate-200 text-slate-600'
                          }`}
                        >
                          {u.ativo ? 'Ativo' : 'Inativo'}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-muted-foreground text-xs">
                        {u.criado_em ? new Date(u.criado_em).toLocaleString('pt-BR') : '—'}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex justify-end gap-2">
                          <button
                            onClick={() => setResetId(u.id)}
                            title="Redefinir senha"
                            className="p-2 rounded-md border border-border hover:bg-sky-50 hover:border-sky-300"
                          >
                            <KeyRound size={14} className="text-sky-700" />
                          </button>
                          <button
                            onClick={() => atualizar(u, { ativo: !u.ativo })}
                            disabled={ehVoce}
                            title={u.ativo ? 'Desativar acesso' : 'Reativar acesso'}
                            className="p-2 rounded-md border border-border hover:bg-amber-50 hover:border-amber-300 disabled:opacity-40 disabled:cursor-not-allowed"
                          >
                            {u.ativo ? (
                              <UserMinus size={14} className="text-amber-700" />
                            ) : (
                              <UserCheck size={14} className="text-emerald-700" />
                            )}
                          </button>
                          <button
                            onClick={() => excluir(u)}
                            disabled={ehVoce}
                            title="Excluir usuário"
                            className="p-2 rounded-md border border-border hover:bg-red-50 hover:border-red-300 disabled:opacity-40 disabled:cursor-not-allowed"
                          >
                            <Trash2 size={14} className="text-red-700" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
                {lista.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-4 py-6 text-center text-sm text-muted-foreground">
                      Nenhum usuário cadastrado ainda.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          )}
        </section>

        {resetId !== null && (
          <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50">
            <div className="bg-white rounded-xl shadow-lg p-6 w-full max-w-sm space-y-3">
              <h3 className="font-bold">Redefinir senha</h3>
              <p className="text-xs text-muted-foreground">
                A senha atual será substituída. Peça ao usuário que troque depois do primeiro login.
              </p>
              <input
                type="password"
                placeholder="Nova senha"
                value={resetSenha}
                onChange={(e) => setResetSenha(e.target.value)}
                className="w-full border border-border rounded-md px-3 py-2 text-sm"
                autoFocus
              />
              <div className="flex justify-end gap-2">
                <button
                  onClick={() => {
                    setResetId(null);
                    setResetSenha('');
                  }}
                  className="px-3 py-2 rounded-md border border-border text-sm"
                >
                  Cancelar
                </button>
                <button
                  onClick={confirmarReset}
                  className="px-3 py-2 rounded-md bg-primary text-white font-bold text-sm"
                >
                  Confirmar
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </>
  );
}
