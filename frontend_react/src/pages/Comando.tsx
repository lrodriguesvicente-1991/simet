import { useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertOctagon,
  AlertTriangle,
  Boxes,
  CheckCircle2,
  Code2,
  Compass,
  Cpu,
  Database,
  Eye,
  EyeOff,
  Flag,
  Gauge,
  Loader2,
  MapPinOff,
  Moon,
  Radar,
  RefreshCw,
  Satellite,
  ShieldAlert,
  ShieldCheck,
  Skull,
  Sparkles,
  Zap,
} from 'lucide-react';
import { api } from '@/lib/api';

type Operacao = 'full' | 'lfp' | 'eci' | 'aci' | 'test';

interface OpcaoOperacao {
  id: Operacao;
  titulo: string;
  subtitulo: string;
  descricao: string;
  icone: typeof Cpu;
  destaque?: boolean;
  usaWorkers: boolean;
}

const OPERACOES: OpcaoOperacao[] = [
  {
    id: 'full',
    titulo: 'Ciclo Completo',
    subtitulo: 'Mapear + Extrair',
    descricao: 'Roda o mapeador e os extratores em paralelo — modo recomendado para uso diário.',
    icone: Sparkles,
    destaque: true,
    usaWorkers: true,
  },
  {
    id: 'lfp',
    titulo: 'Mapeador',
    subtitulo: 'Descobrir anúncios',
    descricao: 'Varre as plataformas e registra novos anúncios na fila para processar.',
    icone: Radar,
    usaWorkers: false,
  },
  {
    id: 'eci',
    titulo: 'Extrator com IA',
    subtitulo: 'Ler e classificar',
    descricao: 'Processa a fila, extrai os dados de cada anúncio e usa IA para classificar o imóvel.',
    icone: Cpu,
    usaWorkers: true,
  },
  {
    id: 'aci',
    titulo: 'Auditor de Anúncios',
    subtitulo: 'Revisar links com erro',
    descricao: 'Revisa anúncios que falharam antes e confirma quais ainda estão no ar.',
    icone: ShieldCheck,
    usaWorkers: false,
  },
  {
    id: 'test',
    titulo: 'Testar Conexão',
    subtitulo: 'Checar banco de dados',
    descricao: 'Verifica rapidamente se a conexão com o banco de dados está ativa.',
    icone: Database,
    usaWorkers: false,
  },
];

function calcularRisco(delayExtra: number): { pct: number; label: string; cor: string } {
  const pct = Math.round(Math.max(3, 70 * Math.exp(-delayExtra / 3.5)));
  if (pct >= 55) return { pct, label: 'Alto', cor: 'text-red-600' };
  if (pct >= 30) return { pct, label: 'Médio', cor: 'text-amber-600' };
  if (pct >= 12) return { pct, label: 'Baixo', cor: 'text-emerald-600' };
  return { pct, label: 'Mínimo', cor: 'text-emerald-700' };
}

// =====================================================================
// Modelagem do estado a partir dos logs
// =====================================================================
type StatusWorker =
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

interface DefStatus {
  label: string;
  icone: typeof Cpu;
  cor: string;
  bg: string;
  barra: string;
  progresso: 0 | 33 | 66 | 100;
  pulsa: boolean;
}

const STATUS: Record<StatusWorker, DefStatus> = {
  ocioso: {
    label: 'Aguardando tarefa',
    icone: Moon,
    cor: 'text-slate-400',
    bg: 'bg-slate-100',
    barra: 'bg-slate-300',
    progresso: 0,
    pulsa: false,
  },
  capturando: {
    label: 'Abrindo anúncio',
    icone: Satellite,
    cor: 'text-sky-600',
    bg: 'bg-sky-50',
    barra: 'bg-sky-500',
    progresso: 33,
    pulsa: true,
  },
  extraindo: {
    label: 'Extraindo dados',
    icone: Cpu,
    cor: 'text-violet-600',
    bg: 'bg-violet-50',
    barra: 'bg-violet-500',
    progresso: 66,
    pulsa: true,
  },
  salvando: {
    label: 'Gravando resultado',
    icone: Database,
    cor: 'text-indigo-600',
    bg: 'bg-indigo-50',
    barra: 'bg-indigo-500',
    progresso: 100,
    pulsa: true,
  },
  sucesso: {
    label: 'Salvo com sucesso',
    icone: CheckCircle2,
    cor: 'text-emerald-600',
    bg: 'bg-emerald-50',
    barra: 'bg-emerald-500',
    progresso: 100,
    pulsa: false,
  },
  descartado: {
    label: 'Descartado',
    icone: AlertTriangle,
    cor: 'text-amber-600',
    bg: 'bg-amber-50',
    barra: 'bg-amber-500',
    progresso: 100,
    pulsa: false,
  },
  linkMorto: {
    label: 'Link morto',
    icone: Skull,
    cor: 'text-slate-500',
    bg: 'bg-slate-100',
    barra: 'bg-slate-400',
    progresso: 100,
    pulsa: false,
  },
  bloqueado: {
    label: 'Pausando para evitar bloqueio',
    icone: ShieldAlert,
    cor: 'text-red-600',
    bg: 'bg-red-50',
    barra: 'bg-red-500',
    progresso: 100,
    pulsa: true,
  },
  fallback: {
    label: 'IA indisponível · usando leitura por regras',
    icone: RefreshCw,
    cor: 'text-orange-600',
    bg: 'bg-orange-50',
    barra: 'bg-orange-500',
    progresso: 66,
    pulsa: true,
  },
  erro: {
    label: 'Erro crítico',
    icone: AlertOctagon,
    cor: 'text-red-700',
    bg: 'bg-red-50',
    barra: 'bg-red-600',
    progresso: 100,
    pulsa: false,
  },
  munAusente: {
    label: 'Município não encontrado',
    icone: MapPinOff,
    cor: 'text-amber-700',
    bg: 'bg-amber-50',
    barra: 'bg-amber-500',
    progresso: 100,
    pulsa: false,
  },
};

interface WorkerState {
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

interface LfpState {
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

interface EventoFeed {
  id: string;
  ts: number;
  worker: number | null;
  status: StatusWorker | 'lfp' | 'system';
  mensagem: string;
  detalhe?: string;
}

// Regex de parsing
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

// =====================================================================
// Componente principal
// =====================================================================
export default function Comando() {
  const [rodando, setRodando] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [operacao, setOperacao] = useState<Operacao>('full');
  const [workers, setWorkers] = useState(3);
  const [modoVisual, setModoVisual] = useState(false);
  const [delayExtra, setDelayExtra] = useState(3);
  const [mostrarCru, setMostrarCru] = useState(false);

  // Estados derivados + acumulados (via ref, sobrevivem a logs saindo do buffer)
  const workersRef = useRef<Map<number, WorkerState>>(new Map());
  const lfpRef = useRef<LfpState>({
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
  });
  const feedRef = useRef<EventoFeed[]>([]);
  const linhasVistas = useRef<Set<string>>(new Set());
  const [tick, setTick] = useState(0);

  // Polling
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const res = await api.get<{ rodando: boolean; logs_recentes: string[] }>('/robo/status');
        setRodando(res.data.rodando);
        setLogs(res.data.logs_recentes || []);
      } catch {
        /* ignora erros transitórios do polling */
      }
    };
    fetchStatus();
    const interval = setInterval(fetchStatus, 2000);
    return () => clearInterval(interval);
  }, []);

  // Reset quando robô para de rodar e depois reinicia
  const prevRodando = useRef(false);
  useEffect(() => {
    if (rodando && !prevRodando.current) {
      workersRef.current = new Map();
      lfpRef.current = {
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
      feedRef.current = [];
      linhasVistas.current = new Set();
    }
    prevRodando.current = rodando;
  }, [rodando]);

  // Parseia logs novos
  useEffect(() => {
    const agora = Date.now();
    let mudou = false;

    for (const linha of logs) {
      if (linhasVistas.current.has(linha)) continue;
      linhasVistas.current.add(linha);
      mudou = true;

      // LFP
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
        lfpRef.current = {
          ...lfpRef.current,
          ativo: false,
          finalizado: true,
          totalInsercoes: parseInt(mFim[1], 10),
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };
        feedRef.current.unshift({
          id: `${agora}-${Math.random()}`,
          ts: agora,
          worker: null,
          status: 'lfp',
          mensagem: `Mapeamento concluído · ${mFim[1]} novos anúncios na fila`,
        });
        continue;
      }

      if (linha.startsWith('[LFP]')) {
        lfpRef.current = { ...lfpRef.current, ultimoLogTs: agora, ultimaLinha: linha };
        continue;
      }

      // Worker
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

      // Log de setup: "Worker 1: modo=IA ..." ou "Worker 1: modo=DETERMINISTICO ..."
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
        let patch: Partial<WorkerState> = {
          status: statusNovo,
          ultimoLogTs: agora,
          ultimaLinha: linha,
        };

        if (statusNovo === 'capturando' || statusNovo === 'extraindo') {
          const mId = body.match(RE_ID_URL);
          if (mId) patch.anuncioId = mId[1];
          if (statusNovo === 'capturando') {
            // Reset de dados do anúncio anterior
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

        // Feed
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
              : `${STATUS[statusNovo].label}${estado.anuncioId ? ` · ${estado.anuncioId}` : ''}`,
          detalhe: body.length > 80 ? body.slice(0, 80) + '…' : undefined,
        };

        // Só adiciona ao feed status que "contam" (evita ruído de subtag)
        if (
          ['capturando', 'sucesso', 'descartado', 'linkMorto', 'bloqueado', 'erro', 'fallback', 'munAusente'].includes(
            statusNovo,
          )
        ) {
          feedRef.current.unshift(evento);
        }
      } else {
        // Tag intermediária: [CORRECAO AREA], [AREA REGEX], [MUN VIA SLUG], [INFO], etc.
        estado = { ...estado, ultimoLogTs: agora, ultimaLinha: linha };
      }

      workersRef.current.set(wid, estado);
    }

    // Limita feed a 200 eventos
    if (feedRef.current.length > 200) feedRef.current = feedRef.current.slice(0, 200);

    if (mudou) setTick((t) => t + 1);
  }, [logs]);

  // Tick periódico para decair workers ativos para "ocioso" após inatividade
  useEffect(() => {
    const iv = setInterval(() => {
      const agora = Date.now();
      let mudou = false;
      for (const [id, w] of workersRef.current) {
        const idade = agora - w.ultimoLogTs;
        // Status terminais (sucesso/descartado/etc) voltam pra ocioso após 3s
        const terminais: StatusWorker[] = ['sucesso', 'descartado', 'linkMorto', 'munAusente', 'erro'];
        if (terminais.includes(w.status) && idade > 3000) {
          workersRef.current.set(id, { ...w, status: 'ocioso' });
          mudou = true;
        }
        // Ativos sem log novo há > 15s também viram ociosos (evita card travado)
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

  const handleIniciar = async () => {
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
  };

  const handleParar = async () => {
    try {
      await api.post('/robo/parar');
    } catch (e) {
      console.error('Erro ao parar robô', e);
    }
  };

  const opAtual = OPERACOES.find((o) => o.id === operacao)!;
  const risco = calcularRisco(delayExtra);

  const workersList = useMemo(() => {
    const arr = Array.from(workersRef.current.values());
    arr.sort((a, b) => a.id - b.id);
    return arr;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tick]);

  const totais = useMemo(() => {
    return workersList.reduce(
      (acc, w) => ({
        sucessos: acc.sucessos + w.sucessos,
        descartes: acc.descartes + w.descartes,
        linksMortos: acc.linksMortos + w.linksMortos,
        erros: acc.erros + w.erros,
      }),
      { sucessos: 0, descartes: 0, linksMortos: 0, erros: 0 },
    );
  }, [workersList]);

  const eventos = feedRef.current;
  const mostrarLFP = operacao === 'full' || operacao === 'lfp';
  const mostrarWorkers = operacao === 'full' || operacao === 'eci';

  return (
    <>
      <header className="bg-white border-b border-border px-8 py-6 flex flex-col shrink-0">
        <h1 className="text-2xl font-bold">Central de Comando</h1>
        <p className="text-muted-foreground text-sm">
          Controle operacional dos robôs de coleta e da IA
        </p>
      </header>

      <div className="flex-1 overflow-auto p-8 space-y-6">
        {/* ==== Passo 1: escolher operação ==== */}
        <section>
          <div className="flex items-center gap-2 mb-3">
            <span className="text-xs font-bold text-muted-foreground uppercase">
              1. Escolha a operação
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
            {OPERACOES.map((o) => {
              const Icone = o.icone;
              const ativo = operacao === o.id;
              return (
                <button
                  key={o.id}
                  onClick={() => !rodando && setOperacao(o.id)}
                  disabled={rodando}
                  className={[
                    'text-left p-4 rounded-xl border transition-all relative',
                    ativo
                      ? 'border-primary bg-primary/5 shadow-md ring-2 ring-primary/20'
                      : 'border-border bg-white hover:border-primary/40 hover:shadow-sm',
                    rodando && !ativo ? 'opacity-40 cursor-not-allowed' : 'cursor-pointer',
                  ].join(' ')}
                >
                  {o.destaque && (
                    <span className="absolute top-2 right-2 bg-amber-100 text-amber-700 text-[10px] font-bold px-2 py-0.5 rounded-full">
                      RECOMENDADO
                    </span>
                  )}
                  <div
                    className={`w-9 h-9 rounded-lg flex items-center justify-center mb-2 ${
                      ativo ? 'bg-primary text-white' : 'bg-muted text-primary'
                    }`}
                  >
                    <Icone size={18} />
                  </div>
                  <div className="font-bold text-sm">{o.titulo}</div>
                  <div className="text-[11px] font-medium text-muted-foreground uppercase tracking-wide">
                    {o.subtitulo}
                  </div>
                  <p className="text-xs text-muted-foreground mt-1.5 leading-snug">{o.descricao}</p>
                </button>
              );
            })}
          </div>
        </section>

        {/* ==== Passo 2: ajustes finos ==== */}
        <section className="bg-white border border-border rounded-xl shadow-sm p-6">
          <div className="flex items-center gap-2 mb-4">
            <span className="text-xs font-bold text-muted-foreground uppercase">
              2. Ajustes finos
            </span>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            {/* Workers */}
            <div className={opAtual.usaWorkers ? '' : 'opacity-40 pointer-events-none'}>
              <label className="flex items-center gap-2 text-sm font-bold mb-1.5">
                <Boxes size={16} className="text-primary" />
                Extratores simultâneos
              </label>
              <p className="text-xs text-muted-foreground mb-3">
                Quantos extratores trabalham ao mesmo tempo. Mais extratores = mais rápido, mais chance de bloqueio.
              </p>
              <div className="flex gap-2">
                {[1, 2, 3, 4, 5].map((w) => (
                  <button
                    key={w}
                    onClick={() => !rodando && setWorkers(w)}
                    disabled={rodando}
                    className={`flex-1 py-2 rounded-md font-bold text-sm border transition ${
                      workers === w
                        ? 'bg-primary text-white border-primary'
                        : 'bg-white border-border hover:border-primary/40'
                    } disabled:opacity-60`}
                  >
                    {w}
                  </button>
                ))}
              </div>
            </div>

            {/* Modo Visual (headless) */}
            <div>
              <label className="flex items-center gap-2 text-sm font-bold mb-1.5">
                {modoVisual ? (
                  <Eye size={16} className="text-primary" />
                ) : (
                  <EyeOff size={16} className="text-muted-foreground" />
                )}
                Modo Visual
              </label>
              <p className="text-xs text-muted-foreground mb-3">
                Mostra as janelas do navegador enquanto o robô trabalha. Útil para acompanhar visualmente — fica mais lento.
              </p>
              <button
                type="button"
                onClick={() => !rodando && setModoVisual((v) => !v)}
                disabled={rodando}
                className={`w-full flex items-center justify-between px-4 py-2.5 rounded-md border transition ${
                  modoVisual
                    ? 'bg-sky-50 border-sky-300 text-sky-800'
                    : 'bg-white border-border text-muted-foreground'
                } disabled:opacity-60`}
              >
                <span className="text-sm font-medium">
                  {modoVisual ? 'Janelas visíveis' : 'Execução em segundo plano'}
                </span>
                <span
                  className={`relative inline-block w-10 h-5 rounded-full transition ${
                    modoVisual ? 'bg-sky-500' : 'bg-gray-300'
                  }`}
                >
                  <span
                    className={`absolute top-0.5 w-4 h-4 rounded-full bg-white transition-all ${
                      modoVisual ? 'left-5' : 'left-0.5'
                    }`}
                  />
                </span>
              </button>
            </div>

            {/* Velocidade vs segurança */}
            <div>
              <label className="flex items-center gap-2 text-sm font-bold mb-1.5">
                <Gauge size={16} className="text-primary" />
                Velocidade vs. Segurança
              </label>
              <p className="text-xs text-muted-foreground mb-3">
                Pausa entre cada anúncio. Mais pausa = menor risco da OLX bloquear o robô.
              </p>
              <div className="flex items-center gap-3">
                <Zap size={14} className="text-amber-500 shrink-0" />
                <input
                  type="range"
                  min={3}
                  max={10}
                  step={0.5}
                  value={delayExtra}
                  onChange={(e) => setDelayExtra(parseFloat(e.target.value))}
                  disabled={rodando}
                  className="flex-1 accent-primary"
                />
                <ShieldCheck size={14} className="text-emerald-600 shrink-0" />
              </div>
              <div className="flex items-center justify-between mt-2 text-xs">
                <span className="font-medium text-foreground">
                  +{delayExtra.toFixed(1)}s por anúncio
                </span>
                <span className={`font-bold ${risco.cor}`}>
                  Risco {risco.label} · ~{risco.pct}%
                </span>
              </div>
            </div>
          </div>

          {/* Botão de ação */}
          <div className="mt-6 pt-6 border-t border-border flex flex-col sm:flex-row items-stretch sm:items-center gap-4">
            <div className="flex-1 text-sm text-muted-foreground">
              <b className="text-foreground">{opAtual.titulo}</b> — {opAtual.descricao}
            </div>
            {!rodando ? (
              <button
                onClick={handleIniciar}
                className="bg-primary hover:bg-primary/90 text-white font-bold py-3 px-6 rounded-lg shadow flex justify-center items-center gap-2 transition min-w-[200px]"
              >
                <Cpu size={18} /> Iniciar operação
              </button>
            ) : (
              <button
                onClick={handleParar}
                className="bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-6 rounded-lg shadow flex justify-center items-center gap-2 transition min-w-[200px]"
              >
                <AlertTriangle size={18} /> Parar operação
              </button>
            )}
          </div>
        </section>

        {/* ==== Painel ao vivo ==== */}
        <section className="space-y-4">
          <div className="flex items-center gap-3">
            <span className="text-xs font-bold text-muted-foreground uppercase">
              3. Operação em tempo real
            </span>
            {rodando && (
              <span className="flex items-center gap-2 text-xs font-bold text-emerald-600">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-2 w-2 rounded-full bg-emerald-400 opacity-75" />
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-emerald-500" />
                </span>
                AO VIVO
              </span>
            )}
            <div className="ml-auto flex items-center gap-3 text-xs text-muted-foreground">
              <span className="flex items-center gap-1">
                <CheckCircle2 size={13} className="text-emerald-600" /> {totais.sucessos}
              </span>
              <span className="flex items-center gap-1">
                <AlertTriangle size={13} className="text-amber-600" /> {totais.descartes}
              </span>
              <span className="flex items-center gap-1">
                <Skull size={13} className="text-slate-500" /> {totais.linksMortos}
              </span>
              {totais.erros > 0 && (
                <span className="flex items-center gap-1">
                  <AlertOctagon size={13} className="text-red-600" /> {totais.erros}
                </span>
              )}
            </div>
          </div>

          {/* LFP */}
          {mostrarLFP && <CardLFP estado={lfpRef.current} rodando={rodando} />}

          {/* Grid de workers */}
          {mostrarWorkers && (
            <div>
              {workersList.length === 0 ? (
                <div className="bg-white border border-dashed border-border rounded-xl p-10 flex flex-col items-center justify-center text-muted-foreground gap-2">
                  <Compass size={28} className="opacity-30" />
                  <span className="text-sm">
                    {rodando
                      ? 'Aguardando os extratores começarem...'
                      : 'Sistema parado. Escolha uma operação acima e clique em Iniciar operação.'}
                  </span>
                </div>
              ) : (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-3">
                  {workersList.map((w) => (
                    <CardWorker key={w.id} estado={w} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Feed de atividade */}
          <FeedAtividade eventos={eventos} rodando={rodando} />

          {/* Console cru opcional */}
          <div>
            <button
              onClick={() => setMostrarCru((v) => !v)}
              className="flex items-center gap-2 text-xs text-muted-foreground hover:text-foreground transition"
            >
              <Code2 size={13} />
              {mostrarCru ? 'Ocultar' : 'Mostrar'} registro técnico ({logs.length} linhas)
            </button>
            {mostrarCru && (
              <pre className="mt-2 bg-slate-900 text-slate-300 text-[11px] font-mono p-3 rounded-lg max-h-64 overflow-auto whitespace-pre-wrap">
                {logs.join('\n')}
              </pre>
            )}
          </div>
        </section>
      </div>
    </>
  );
}

// =====================================================================
// Sub-componentes
// =====================================================================
function CardWorker({ estado }: { estado: WorkerState }) {
  const def = STATUS[estado.status];
  const Icone = def.icone;
  const ativo = estado.status !== 'ocioso';

  return (
    <div
      className={`relative border rounded-xl overflow-hidden transition-shadow ${
        ativo ? 'border-border shadow-sm bg-white' : 'border-border/60 bg-slate-50'
      }`}
    >
      {/* Cabeçalho */}
      <div className="flex items-center justify-between px-3 pt-3 pb-2">
        <div className="flex items-center gap-2">
          <span className="text-sm font-bold">Worker {estado.id}</span>
          {estado.modo && (
            <span
              className={`text-[9px] font-bold px-1.5 py-0.5 rounded ${
                estado.modo === 'ia' ? 'bg-violet-100 text-violet-700' : 'bg-slate-200 text-slate-600'
              }`}
            >
              {estado.modo === 'ia' ? 'IA' : 'REGRAS'}
            </span>
          )}
        </div>
        <div className={`p-1.5 rounded-full ${def.bg} ${def.cor}`}>
          {def.pulsa ? (
            <Loader2 size={14} className="animate-spin" />
          ) : (
            <Icone size={14} />
          )}
        </div>
      </div>

      {/* Status + detalhes */}
      <div className="px-3 pb-3">
        <div className={`text-xs font-bold ${def.cor} mb-1`}>{def.label}</div>

        {/* Info do anúncio */}
        <div className="min-h-[42px] text-[11px] text-muted-foreground leading-snug">
          {estado.status === 'sucesso' && estado.municipio ? (
            <>
              <div className="font-semibold text-foreground">
                {estado.areaHa ?? '—'} ha · {formatarValor(estado.valor)}
              </div>
              <div>
                {estado.municipio}/{estado.uf} · conf {estado.confianca ?? '—'}
              </div>
            </>
          ) : estado.anuncioId ? (
            <>
              <div className="font-mono text-[10px]">ID {estado.anuncioId}</div>
              {estado.municipio && (
                <div>
                  {estado.municipio}/{estado.uf}
                </div>
              )}
            </>
          ) : (
            <span className="italic opacity-60">—</span>
          )}
        </div>

        {/* Barra de progresso segmentada: capturando -> extraindo -> salvando */}
        <div className="mt-2 flex gap-0.5 h-1.5 rounded-full overflow-hidden bg-slate-100">
          <SegmentoBarra ativo={def.progresso >= 33} cor={def.barra} animado={estado.status === 'capturando'} />
          <SegmentoBarra ativo={def.progresso >= 66} cor={def.barra} animado={estado.status === 'extraindo' || estado.status === 'fallback'} />
          <SegmentoBarra ativo={def.progresso >= 100} cor={def.barra} animado={estado.status === 'salvando'} />
        </div>

        {/* Contadores */}
        <div className="mt-2 pt-2 border-t border-border/60 flex items-center gap-2 text-[10px] font-medium">
          <span className="flex items-center gap-0.5 text-emerald-600">
            <CheckCircle2 size={11} /> {estado.sucessos}
          </span>
          <span className="flex items-center gap-0.5 text-amber-600">
            <AlertTriangle size={11} /> {estado.descartes}
          </span>
          <span className="flex items-center gap-0.5 text-slate-500">
            <Skull size={11} /> {estado.linksMortos}
          </span>
          {estado.erros > 0 && (
            <span className="flex items-center gap-0.5 text-red-600">
              <AlertOctagon size={11} /> {estado.erros}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

function SegmentoBarra({
  ativo,
  cor,
  animado,
}: {
  ativo: boolean;
  cor: string;
  animado: boolean;
}) {
  return (
    <div className="flex-1 bg-slate-200 overflow-hidden">
      <div
        className={`h-full ${ativo ? cor : 'bg-transparent'} ${animado ? 'animate-pulse' : ''}`}
      />
    </div>
  );
}

function CardLFP({ estado, rodando }: { estado: LfpState; rodando: boolean }) {
  const ativo = estado.ativo && rodando;
  const pct =
    estado.paginasTotal && estado.paginasTotal > 0
      ? Math.min(100, Math.round((estado.pagina / estado.paginasTotal) * 100))
      : null;

  return (
    <div className="bg-white border border-border rounded-xl p-4 shadow-sm">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className={`p-2 rounded-lg ${ativo ? 'bg-sky-50 text-sky-600' : 'bg-slate-100 text-slate-400'}`}>
            {ativo ? <Radar size={18} className="animate-pulse" /> : <Radar size={18} />}
          </div>
          <div>
            <div className="font-bold text-sm">Mapeador de anúncios</div>
            <div className="text-[11px] text-muted-foreground">
              {estado.finalizado
                ? 'Varredura concluída'
                : ativo
                ? `Varrendo ${estado.uf ?? '—'} · ${estado.plataforma ?? '—'}`
                : rodando
                ? 'Preparando a varredura...'
                : 'Parado'}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-4 text-xs">
          {estado.uf && (
            <div className="flex items-center gap-1 font-bold">
              <Flag size={13} className="text-sky-600" />
              {estado.uf}
            </div>
          )}
          <div className="text-right">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Novos</div>
            <div className="font-bold text-sky-600">{estado.totalInsercoes}</div>
          </div>
        </div>
      </div>

      {/* Progresso */}
      <div className="space-y-1">
        <div className="flex items-center justify-between text-[11px] text-muted-foreground">
          <span>
            {estado.pagina > 0
              ? `Página ${estado.pagina}${estado.paginasTotal ? ` / ${estado.paginasTotal}` : ''}`
              : 'Nenhuma página em andamento'}
          </span>
          {estado.amostrasUltima > 0 && (
            <span>
              última página: {estado.amostrasUltima} anúncios · {estado.insercoesUltima} novos
            </span>
          )}
        </div>
        <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
          {pct !== null ? (
            <div
              className="h-full bg-sky-500 transition-all"
              style={{ width: `${pct}%` }}
            />
          ) : ativo ? (
            <div className="h-full bg-gradient-to-r from-sky-200 via-sky-500 to-sky-200 bg-[length:200%_100%] animate-[shimmer_1.5s_infinite]" />
          ) : (
            <div className="h-full bg-transparent" />
          )}
        </div>
      </div>
    </div>
  );
}

function FeedAtividade({ eventos, rodando }: { eventos: EventoFeed[]; rodando: boolean }) {
  if (eventos.length === 0) {
    return (
      <div className="bg-white border border-border rounded-xl p-6 text-center text-xs text-muted-foreground">
        {rodando
          ? 'Aguardando a primeira atividade...'
          : 'Ainda sem atividade — inicie uma operação para acompanhar aqui.'}
      </div>
    );
  }

  return (
    <div className="bg-white border border-border rounded-xl shadow-sm overflow-hidden">
      <div className="px-4 py-2 border-b border-border flex items-center gap-2 text-xs font-bold text-muted-foreground uppercase tracking-wide">
        Histórico em tempo real
        <span className="text-muted-foreground/60 font-normal normal-case">
          ({eventos.length} eventos)
        </span>
      </div>
      <div className="max-h-96 overflow-y-auto divide-y divide-border/60">
        {eventos.slice(0, 80).map((ev) => {
          const def = ev.status === 'lfp' || ev.status === 'system' ? null : STATUS[ev.status as StatusWorker];
          const Icone = def?.icone ?? Radar;
          const cor = def?.cor ?? 'text-sky-600';
          const bg = def?.bg ?? 'bg-sky-50';
          return (
            <div key={ev.id} className="flex items-start gap-3 px-4 py-2 hover:bg-slate-50 transition">
              <div className={`mt-0.5 p-1.5 rounded-full ${bg} ${cor} shrink-0`}>
                <Icone size={12} />
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-xs">
                  {ev.worker !== null && (
                    <span className="font-bold text-foreground mr-1.5">Worker {ev.worker}</span>
                  )}
                  {ev.status === 'lfp' && <span className="font-bold text-sky-700 mr-1.5">Mapeador</span>}
                  <span className="text-muted-foreground">{ev.mensagem}</span>
                </div>
                {ev.detalhe && (
                  <div className="text-[10px] text-muted-foreground/70 mt-0.5 font-mono">
                    {ev.detalhe}
                  </div>
                )}
              </div>
              <div className="text-[10px] text-muted-foreground/70 font-mono shrink-0 pt-1">
                {new Date(ev.ts).toLocaleTimeString('pt-BR', { hour12: false })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
