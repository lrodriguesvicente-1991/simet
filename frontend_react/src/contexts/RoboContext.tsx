import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from 'react';
import { api } from '@/lib/api';

// =====================================================================
// Tipos expostos (consumidos por Comando.tsx)
// =====================================================================
export type Operacao = 'full' | 'lfp' | 'eci' | 'aci' | 'test';

export type StatusWorker =
  | 'ocioso'
  | 'capturando'
  | 'extraindo'
  | 'salvando'
  | 'sucesso'
  | 'descartado'
  | 'linkMorto'
  | 'bloqueado'
  | 'fallback'
  | 'erro'
  | 'munAusente';

export interface WorkerState {
  id: number;
  status: StatusWorker;
  modo: 'ia' | 'deterministico' | null;
  anuncioId: string | null;
  municipio: string | null;
  uf: string | null;
  areaHa: number | null;
  valor: number | null;
  confianca: number | null;
  sucessos: number;
  descartes: number;
  linksMortos: number;
  erros: number;
  ultimoLogTs: number;
  ultimaLinha: string;
}

export interface LfpState {
  ativo: boolean;
  uf: string | null;
  plataforma: string | null;
  pagina: number;
  paginasTotal: number | null;
  amostrasUltima: number;
  insercoesUltima: number;
  totalInsercoes: number;
  ultimoLogTs: number;
  ultimaLinha: string;
  finalizado: boolean;
}

export interface EventoFeed {
  id: string;
  ts: number;
  worker: number | null;
  status: StatusWorker | 'lfp' | 'system';
  mensagem: string;
  detalhe?: string;
}

export interface Totais {
  sucessos: number;
  descartes: number;
  linksMortos: number;
  erros: number;
}

// =====================================================================
// Regex e utilitários de parsing (isolados do componente de UI)
// =====================================================================
const RE_WORKER = /^Worker\s+(\d+):\s+(.*)$/;
const RE_TAG = /^\[([^\]]+)\]\s*(.*)$/;
const RE_SUCESSO = /ID\s+(\S+)\s+\|\s+([\d.]+)ha\s+\|\s+R\$([\d.]+)\s+\|\s+([^/]+)\/(\w+)\s+\|\s+.*conf:(\d+)/;
const RE_ID_URL = /ID\s+(\S+)(?:\s+\|\s+(\S+))?/;
const RE_ID_MUN = /ID\s+(\S+)\s+\|\s+([^/]+)\/(\w+)/;
const RE_MODO = /modo=(IA|DETERMINISTICO)/i;
const RE_LFP_VARRE = /^\[LFP\]\s+Varredura iniciada no estado\s+(\w+)\s+\|\s+Plataforma:\s+(\w+)\s+\|\s+Pág:\s+(\d+)\/(\S+)/;
const RE_LFP_LEITURA = /^\[LFP\]\s+Leitura conclu[ií]da:\s+(\w+)\s+\(Pág\s+(\d+)\)\s+\|\s+Amostras:\s+(\d+)\s+\|\s+Inser[çc][ãa]o[ae]s:\s+(\d+)/;
const RE_LFP_FIM_TOTAL = /^\[LFP\]\s+Opera[çc][ãa]o Finalizada\.\s+Total de novas inser[çc][õo]es:\s+(\d+)/;

function interpretarTag(tag: string): StatusWorker | null {
  const t = tag.toUpperCase().replace(/\s+/g, ' ').trim();
  if (t === 'PROCESSANDO') return 'capturando';
  if (t === 'EXTRAINDO') return 'extraindo';
  if (t === 'SALVANDO') return 'salvando';
  if (t === 'SUCESSO') return 'sucesso';
  if (t === 'IMPLAUSIVEL' || t === 'CONFIANCA BAIXA' || t === 'VAZIO') return 'descartado';
  if (t === 'LINK MORTO') return 'linkMorto';
  if (t === 'BLOQUEIO') return 'bloqueado';
  if (t === 'IA->FALLBACK' || t === 'IA FALHOU') return 'fallback';
  if (t === 'ERRO CRITICO') return 'erro';
  if (t === 'MUN NAO ENCONTRADO') return 'munAusente';
  return null;
}

function formatarValor(v: number | null): string {
  if (v === null || !isFinite(v)) return '—';
  if (v >= 1_000_000) return `R$ ${(v / 1_000_000).toFixed(1).replace('.', ',')}M`;
  if (v >= 1_000) return `R$ ${(v / 1_000).toFixed(0)}k`;
  return `R$ ${v.toFixed(0)}`;
}

const LFP_INICIAL: LfpState = {
  ativo: false,
  uf: null,
  plataforma: null,
  pagina: 0,
  paginasTotal: null,
  amostrasUltima: 0,
  insercoesUltima: 0,
  totalInsercoes: 0,
  ultimoLogTs: 0,
  ultimaLinha: '',
  finalizado: false,
};

// =====================================================================
// Contexto
// =====================================================================
interface RoboContextValue {
  // runtime (polling + parsing)
  rodando: boolean;
  circuitBreaker: boolean;
  circuitBreakerMsg: string | null;
  logs: string[];
  workersList: WorkerState[];
  lfpState: LfpState;
  eventos: EventoFeed[];
  totais: Totais;

  // form (sobrevive à troca de aba)
  operacao: Operacao;
  setOperacao: (o: Operacao) => void;
  workers: number;
  setWorkers: (n: number) => void;
  modoVisual: boolean;
  setModoVisual: React.Dispatch<React.SetStateAction<boolean>>;
  delayExtra: number;
  setDelayExtra: (n: number) => void;
  mostrarCru: boolean;
  setMostrarCru: React.Dispatch<React.SetStateAction<boolean>>;

  // ações
  iniciar: () => Promise<void>;
  parar: () => Promise<void>;
  reconhecerAlarme: () => Promise<void>;
}

const RoboContext = createContext<RoboContextValue | null>(null);

export function RoboProvider({ children }: { children: ReactNode }) {
  // Runtime state
  const [rodando, setRodando] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [circuitBreaker, setCircuitBreaker] = useState(false);
  const [circuitBreakerMsg, setCircuitBreakerMsg] = useState<string | null>(null);

  // Form state (persiste entre navegações)
  const [operacao, setOperacao] = useState<Operacao>('full');
  const [workers, setWorkers] = useState(3);
  const [modoVisual, setModoVisual] = useState(false);
  const [delayExtra, setDelayExtra] = useState(3);
  const [mostrarCru, setMostrarCru] = useState(false);

  // Refs de parsing (sobrevivem ao buffer de logs)
  const workersRef = useRef<Map<number, WorkerState>>(new Map());
  const lfpRef = useRef<LfpState>({ ...LFP_INICIAL });
  const feedRef = useRef<EventoFeed[]>([]);
  const linhasVistas = useRef<Set<string>>(new Set());
  const lfpNotificadoRef = useRef(false);
  const prevRodando = useRef(false);
  const [tick, setTick] = useState(0);

  // --------------------------------------------------------------------
  // Polling /robo/status
  // --------------------------------------------------------------------
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await api.get<{
          rodando: boolean;
          logs_recentes: string[];
          circuit_breaker?: boolean;
          circuit_breaker_msg?: string | null;
        }>('/robo/status');
        setRodando(res.data.rodando);
        setLogs(res.data.logs_recentes || []);
        setCircuitBreaker(!!res.data.circuit_breaker);
        setCircuitBreakerMsg(res.data.circuit_breaker_msg ?? null);
      } catch {
        /* ignora erros transitórios do polling */
      }
    };
    fetchStatus();
    const interval = setInterval(fetchStatus, 2000);
    return () => clearInterval(interval);
  }, []);

  // --------------------------------------------------------------------
  // Reset quando robô para de rodar e depois reinicia
  // --------------------------------------------------------------------
  useEffect(() => {
    if (rodando && !prevRodando.current) {
      workersRef.current = new Map();
      lfpRef.current = { ...LFP_INICIAL };
      feedRef.current = [];
      linhasVistas.current = new Set();
      lfpNotificadoRef.current = false;
      if (typeof Notification !== 'undefined' && Notification.permission === 'default') {
        Notification.requestPermission().catch(() => {});
      }
    }
    prevRodando.current = rodando;
  }, [rodando]);

  // --------------------------------------------------------------------
  // Parseia logs novos (só linhas inéditas)
  // --------------------------------------------------------------------
  useEffect(() => {
    const agora = Date.now();
    let mudou = false;

    for (const linha of logs) {
      if (linhasVistas.current.has(linha)) continue;
      linhasVistas.current.add(linha);
      mudou = true;

      const mVarre = linha.match(RE_LFP_VARRE);
      if (mVarre) {
        const paginasTotal = mVarre[4] === 'sem limite' ? null : parseInt(mVarre[4], 10);
        lfpRef.current = {
          ...lfpRef.current,
          ativo: true,
          uf: mVarre[1],
          plataforma: mVarre[2],
          pagina: parseInt(mVarre[3], 10),
          paginasTotal,
          ultimoLogTs: agora,
          ultimaLinha: linha,
          finalizado: false,
        };
        feedRef.current.unshift({
          id: `${agora}-${Math.random()}`,
          ts: agora,
          worker: null,
          status: 'lfp',
          mensagem: `Varrendo ${mVarre[1]} · ${mVarre[2]} · Página ${mVarre[3]}/${mVarre[4]}`,
        });
        continue;
      }

      const mLeitura = linha.match(RE_LFP_LEITURA);
      if (mLeitura) {
        const inseridos = parseInt(mLeitura[4], 10);
        const amostras = parseInt(mLeitura[3], 10);
        lfpRef.current = {
          ...lfpRef.current,
          ativo: true,
          amostrasUltima: amostras,
          insercoesUltima: inseridos,
          totalInsercoes: lfpRef.current.totalInsercoes + inseridos,
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };
        feedRef.current.unshift({
          id: `${agora}-${Math.random()}`,
          ts: agora,
          worker: null,
          status: 'lfp',
          mensagem: `${mLeitura[1]} pág ${mLeitura[2]} · ${amostras} anúncios · ${inseridos} novos`,
        });
        continue;
      }

      const mFim = linha.match(RE_LFP_FIM_TOTAL);
      if (mFim) {
        const totalNovos = parseInt(mFim[1], 10);
        lfpRef.current = {
          ...lfpRef.current,
          ativo: false,
          finalizado: true,
          totalInsercoes: totalNovos,
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };
        feedRef.current.unshift({
          id: `${agora}-${Math.random()}`,
          ts: agora,
          worker: null,
          status: 'lfp',
          mensagem: `Mapeamento concluído · ${totalNovos} novos anúncios na fila`,
        });
        if (!lfpNotificadoRef.current) {
          lfpNotificadoRef.current = true;
          if (typeof Notification !== 'undefined' && Notification.permission === 'granted') {
            try {
              new Notification('SIMET · Mapeador concluído', {
                body: `${totalNovos} novos anúncios adicionados à fila.`,
                icon: '/favicon.png',
                tag: 'simet-lfp-fim',
              });
            } catch {
              /* navegador pode bloquear em contextos não-seguros */
            }
          }
        }
        continue;
      }

      if (linha.startsWith('[LFP]')) {
        lfpRef.current = { ...lfpRef.current, ultimoLogTs: agora, ultimaLinha: linha };
        continue;
      }

      const mW = linha.match(RE_WORKER);
      if (!mW) continue;

      const wid = parseInt(mW[1], 10);
      const resto = mW[2];
      let estado = workersRef.current.get(wid);
      if (!estado) {
        estado = {
          id: wid,
          status: 'ocioso',
          modo: null,
          anuncioId: null,
          municipio: null,
          uf: null,
          areaHa: null,
          valor: null,
          confianca: null,
          sucessos: 0,
          descartes: 0,
          linksMortos: 0,
          erros: 0,
          ultimoLogTs: 0,
          ultimaLinha: '',
        };
      }

      const mModo = resto.match(RE_MODO);
      if (mModo) {
        estado = {
          ...estado,
          modo: mModo[1].toUpperCase() === 'IA' ? 'ia' : 'deterministico',
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };
        workersRef.current.set(wid, estado);
        continue;
      }

      const mTag = resto.match(RE_TAG);
      if (!mTag) {
        estado = { ...estado, ultimoLogTs: agora, ultimaLinha: linha };
        workersRef.current.set(wid, estado);
        continue;
      }

      const tag = mTag[1];
      const body = mTag[2];
      const statusNovo = interpretarTag(tag);

      if (statusNovo) {
        const patch: Partial<WorkerState> = {
          status: statusNovo,
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };

        if (statusNovo === 'capturando' || statusNovo === 'extraindo') {
          const mId = body.match(RE_ID_URL);
          if (mId) patch.anuncioId = mId[1];
          if (statusNovo === 'capturando') {
            patch.municipio = null;
            patch.uf = null;
            patch.areaHa = null;
            patch.valor = null;
            patch.confianca = null;
          }
        } else if (statusNovo === 'salvando') {
          const mIdMun = body.match(RE_ID_MUN);
          if (mIdMun) {
            patch.anuncioId = mIdMun[1];
            patch.municipio = mIdMun[2].trim();
            patch.uf = mIdMun[3];
          }
        } else if (statusNovo === 'sucesso') {
          const mS = body.match(RE_SUCESSO);
          if (mS) {
            patch.anuncioId = mS[1];
            patch.areaHa = parseFloat(mS[2]);
            patch.valor = parseFloat(mS[3]);
            patch.municipio = mS[4].trim();
            patch.uf = mS[5];
            patch.confianca = parseInt(mS[6], 10);
          }
          patch.sucessos = estado.sucessos + 1;
        } else if (statusNovo === 'descartado' || statusNovo === 'munAusente') {
          patch.descartes = estado.descartes + 1;
        } else if (statusNovo === 'linkMorto') {
          patch.linksMortos = estado.linksMortos + 1;
        } else if (statusNovo === 'erro') {
          patch.erros = estado.erros + 1;
        }

        estado = { ...estado, ...patch };

        const evento: EventoFeed = {
          id: `${agora}-${wid}-${Math.random()}`,
          ts: agora,
          worker: wid,
          status: statusNovo,
          mensagem:
            statusNovo === 'sucesso' && estado.municipio
              ? `Salvou ${estado.municipio}/${estado.uf ?? ''} · ${estado.areaHa ?? '—'} ha · ${formatarValor(
                  estado.valor,
                )} · conf ${estado.confianca ?? '—'}`
              : statusNovo === 'capturando' && estado.anuncioId
              ? `Capturando anúncio ${estado.anuncioId}`
              : statusNovo === 'extraindo' && estado.anuncioId
              ? `Extraindo ${estado.anuncioId}`
              : statusNovo === 'salvando' && estado.municipio
              ? `Salvando em ${estado.municipio}/${estado.uf ?? ''}`
              : `${tag}${estado.anuncioId ? ` · ${estado.anuncioId}` : ''}`,
          detalhe: body.length > 80 ? body.slice(0, 80) + '…' : undefined,
        };

        if (
          ['capturando', 'sucesso', 'descartado', 'linkMorto', 'bloqueado', 'erro', 'fallback', 'munAusente'].includes(
            statusNovo,
          )
        ) {
          feedRef.current.unshift(evento);
        }
      } else {
        estado = { ...estado, ultimoLogTs: agora, ultimaLinha: linha };
      }

      workersRef.current.set(wid, estado);
    }

    if (feedRef.current.length > 200) feedRef.current = feedRef.current.slice(0, 200);

    if (mudou) setTick((t) => t + 1);
  }, [logs]);

  // --------------------------------------------------------------------
  // Decay: status terminais voltam a "ocioso" após inatividade
  // --------------------------------------------------------------------
  useEffect(() => {
    const iv = setInterval(() => {
      const agora = Date.now();
      let mudou = false;
      for (const [id, w] of workersRef.current) {
        const idade = agora - w.ultimoLogTs;
        const terminais: StatusWorker[] = ['sucesso', 'descartado', 'linkMorto', 'munAusente', 'erro'];
        if (terminais.includes(w.status) && idade > 3000) {
          workersRef.current.set(id, { ...w, status: 'ocioso' });
          mudou = true;
        }
        const ativos: StatusWorker[] = ['capturando', 'extraindo', 'salvando', 'fallback'];
        if (ativos.includes(w.status) && idade > 15000) {
          workersRef.current.set(id, { ...w, status: 'ocioso' });
          mudou = true;
        }
      }
      if (mudou) setTick((t) => t + 1);
    }, 1000);
    return () => clearInterval(iv);
  }, []);

  // --------------------------------------------------------------------
  // Derivados
  // --------------------------------------------------------------------
  const workersList = useMemo(() => {
    const arr = Array.from(workersRef.current.values());
    arr.sort((a, b) => a.id - b.id);
    return arr;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick]);

  const totais = useMemo<Totais>(
    () =>
      workersList.reduce(
        (acc, w) => ({
          sucessos: acc.sucessos + w.sucessos,
          descartes: acc.descartes + w.descartes,
          linksMortos: acc.linksMortos + w.linksMortos,
          erros: acc.erros + w.erros,
        }),
        { sucessos: 0, descartes: 0, linksMortos: 0, erros: 0 },
      ),
    [workersList],
  );

  // --------------------------------------------------------------------
  // Ações
  // --------------------------------------------------------------------
  const iniciar = useCallback(async () => {
    try {
      await api.post('/robo/iniciar', {
        task: operacao,
        workers,
        headless: !modoVisual,
        delay_extra: delayExtra,
      });
    } catch (e) {
      console.error('Erro ao iniciar robô', e);
    }
  }, [operacao, workers, modoVisual, delayExtra]);

  const parar = useCallback(async () => {
    try {
      await api.post('/robo/parar');
    } catch (e) {
      console.error('Erro ao parar robô', e);
    }
  }, []);

  const reconhecerAlarme = useCallback(async () => {
    try {
      await api.post('/robo/reconhecer-alarme');
      setCircuitBreaker(false);
      setCircuitBreakerMsg(null);
    } catch (e) {
      console.error('Erro ao reconhecer alarme', e);
    }
  }, []);

  return (
    <RoboContext.Provider
      value={{
        rodando,
        circuitBreaker,
        circuitBreakerMsg,
        logs,
        workersList,
        lfpState: lfpRef.current,
        eventos: feedRef.current,
        totais,
        operacao,
        setOperacao,
        workers,
        setWorkers,
        modoVisual,
        setModoVisual,
        delayExtra,
        setDelayExtra,
        mostrarCru,
        setMostrarCru,
        iniciar,
        parar,
        reconhecerAlarme,
      }}
    >
      {children}
    </RoboContext.Provider>
  );
}

export function useRobo() {
  const ctx = useContext(RoboContext);
  if (!ctx) throw new Error('useRobo deve ser usado dentro de <RoboProvider>');
  return ctx;
}

export { formatarValor };
