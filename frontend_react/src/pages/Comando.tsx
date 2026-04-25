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
import { useState } from 'react';
import {
  formatarValor,
  useRobo,
  type AciState,
  type EventoFeed,
  type LfpState,
  type Operacao,
  type StatusWorker,
  type TestState,
  type WorkerState,
} from '@/contexts/RoboContext';

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
    label: 'Anúncio removido',
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
    label: 'Falha ao processar',
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

// =====================================================================
// Componente principal
// =====================================================================
export default function Comando() {
  const {
    rodando,
    parando,
    circuitBreaker,
    circuitBreakerMsg,
    logs,
    workersList,
    lfpState,
    aciState,
    testState,
    eventos,
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
    pararForcado,
    reconhecerAlarme,
  } = useRobo();

  const [confirmarParada, setConfirmarParada] = useState(false);

  const opAtual = OPERACOES.find((o) => o.id === operacao)!;
  const risco = calcularRisco(delayExtra);
  const mostrarLFP = operacao === 'full' || operacao === 'lfp';
  const mostrarWorkers = operacao === 'full' || operacao === 'eci';
  const mostrarACI = operacao === 'aci';
  const mostrarTest = operacao === 'test';

  return (
    <>
      <header className="bg-white border-b border-border px-8 py-6 flex flex-col shrink-0">
        <h1 className="text-2xl font-bold">Central de Comando</h1>
        <p className="text-muted-foreground text-sm">
          Controle operacional dos robôs de coleta e da IA
        </p>
      </header>

      <div className="flex-1 overflow-auto p-8 space-y-6">
        {circuitBreaker && (
          <div className="bg-red-600 text-white rounded-xl shadow-lg border border-red-800 p-5 flex items-start gap-4 animate-pulse">
            <AlertOctagon size={36} className="shrink-0 mt-0.5" />
            <div className="flex-1">
              <div className="font-bold text-lg">Alarme do extrator disparado</div>
              <p className="text-sm mt-1 text-red-50">
                O sistema processou muitos anúncios seguidos sem conseguir salvar nenhum. O robô foi
                parado automaticamente para evitar queima de IP. Investigue antes de reiniciar —
                provável mudança no layout da OLX ou bloqueio em andamento.
              </p>
              {circuitBreakerMsg && (
                <div className="mt-2 text-[11px] font-mono bg-red-900/40 px-2 py-1 rounded">
                  {circuitBreakerMsg}
                </div>
              )}
            </div>
            <button
              onClick={reconhecerAlarme}
              className="bg-white text-red-700 font-bold px-4 py-2 rounded-md shadow hover:bg-red-50 transition shrink-0"
            >
              Reconhecer
            </button>
          </div>
        )}

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
                onClick={iniciar}
                className="bg-primary hover:bg-primary/90 text-white font-bold py-3 px-6 rounded-lg shadow flex justify-center items-center gap-2 transition min-w-[200px]"
              >
                <Cpu size={18} /> Iniciar operação
              </button>
            ) : parando ? (
              <button
                type="button"
                disabled
                className="bg-amber-500 text-white font-bold py-3 px-6 rounded-lg shadow flex justify-center items-center gap-2 min-w-[200px] cursor-wait"
              >
                <Loader2 size={18} className="animate-spin" />
                Encerrando…
              </button>
            ) : (
              <button
                onClick={() => setConfirmarParada(true)}
                className="bg-red-600 hover:bg-red-700 text-white font-bold py-3 px-6 rounded-lg shadow flex justify-center items-center gap-2 transition min-w-[200px]"
              >
                <AlertTriangle size={18} /> Parar operação
              </button>
            )}
          </div>
        </section>

        {/* Modal de confirmação de parada */}
        {confirmarParada && (
          <div
            role="dialog"
            aria-modal="true"
            className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4"
            onClick={() => setConfirmarParada(false)}
          >
            <div
              className="bg-white rounded-xl shadow-2xl max-w-md w-full p-6"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex items-start gap-3 mb-4">
                <div className="bg-red-50 text-red-600 p-2 rounded-full shrink-0">
                  <AlertTriangle size={20} />
                </div>
                <div>
                  <h3 className="font-bold text-lg">Encerrar a operação?</h3>
                  <p className="text-sm text-muted-foreground mt-1">
                    Os workers vão terminar o anúncio atual e depois encerrar —
                    nada fica pela metade na base. Pode levar alguns segundos até
                    todos pararem.
                  </p>
                </div>
              </div>
              <div className="flex justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setConfirmarParada(false)}
                  className="px-4 py-2 rounded-md text-sm font-medium bg-muted hover:bg-muted/70 transition"
                >
                  Cancelar
                </button>
                <button
                  type="button"
                  onClick={() => {
                    setConfirmarParada(false);
                    parar();
                  }}
                  className="px-4 py-2 rounded-md text-sm font-bold bg-red-600 hover:bg-red-700 text-white transition"
                >
                  Sim, encerrar
                </button>
              </div>
              <div className="mt-4 pt-3 border-t border-border text-[11px] text-muted-foreground">
                Se travar e precisar encerrar à força:
                <button
                  type="button"
                  onClick={() => {
                    setConfirmarParada(false);
                    pararForcado();
                  }}
                  className="ml-2 underline hover:text-red-600"
                >
                  parar à força (kill)
                </button>
              </div>
            </div>
          </div>
        )}

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
          {mostrarLFP && <CardLFP estado={lfpState} rodando={rodando} />}

          {/* ACI */}
          {mostrarACI && <CardACI estado={aciState} rodando={rodando} />}

          {/* Test (3 cards: validos / fila / erros) */}
          {mostrarTest && <CardsTest estado={testState} rodando={rodando} />}

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

  const iconeWrapCls = estado.finalizado
    ? 'bg-emerald-50 text-emerald-600'
    : ativo
    ? 'bg-sky-50 text-sky-600'
    : 'bg-slate-100 text-slate-400';

  const cardCls = estado.finalizado
    ? 'bg-white border border-emerald-300 ring-1 ring-emerald-200 rounded-xl p-4 shadow-sm'
    : 'bg-white border border-border rounded-xl p-4 shadow-sm';

  return (
    <div className={cardCls}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className={`p-2 rounded-lg ${iconeWrapCls}`}>
            {estado.finalizado ? (
              <CheckCircle2 size={18} />
            ) : ativo ? (
              <Radar size={18} className="animate-pulse" />
            ) : (
              <Radar size={18} />
            )}
          </div>
          <div>
            <div className="font-bold text-sm flex items-center gap-2">
              Mapeador de anúncios
              {estado.finalizado && (
                <span className="text-[10px] font-bold uppercase tracking-wide bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded">
                  Concluído
                </span>
              )}
            </div>
            <div className={`text-[11px] ${estado.finalizado ? 'text-emerald-700 font-semibold' : 'text-muted-foreground'}`}>
              {estado.finalizado
                ? `Varredura concluída · ${estado.totalInsercoes} novos na fila`
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

function CardACI({ estado, rodando }: { estado: AciState; rodando: boolean }) {
  const ativo = estado.ativo && rodando;
  const pct =
    estado.totalAuditar > 0
      ? Math.min(100, Math.round((estado.auditados / estado.totalAuditar) * 100))
      : null;

  const iconeWrapCls = estado.finalizado
    ? 'bg-emerald-50 text-emerald-600'
    : ativo
    ? 'bg-amber-50 text-amber-600'
    : 'bg-slate-100 text-slate-400';

  const cardCls = estado.finalizado
    ? 'bg-white border border-emerald-300 ring-1 ring-emerald-200 rounded-xl p-4 shadow-sm'
    : 'bg-white border border-border rounded-xl p-4 shadow-sm';

  const totalReciclados = estado.reciclados + estado.recicladosIA;

  return (
    <div className={cardCls}>
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <div className={`p-2 rounded-lg ${iconeWrapCls}`}>
            {estado.finalizado ? (
              <CheckCircle2 size={18} />
            ) : ativo ? (
              <ShieldCheck size={18} className="animate-pulse" />
            ) : (
              <ShieldCheck size={18} />
            )}
          </div>
          <div>
            <div className="font-bold text-sm flex items-center gap-2">
              Auditor de anúncios
              {estado.finalizado && (
                <span className="text-[10px] font-bold uppercase tracking-wide bg-emerald-100 text-emerald-700 px-1.5 py-0.5 rounded">
                  Concluído
                </span>
              )}
            </div>
            <div className={`text-[11px] ${estado.finalizado ? 'text-emerald-700 font-semibold' : 'text-muted-foreground'}`}>
              {estado.finalizado
                ? `Auditoria concluída · ${estado.auditados} anúncios revisados`
                : ativo && estado.filIdAtual
                ? `Verificando #${estado.filIdAtual}${estado.motivoAtual ? ` · ${estado.motivoAtual}` : ''}`
                : ativo
                ? 'Iniciando auditoria...'
                : rodando
                ? 'Preparando a auditoria...'
                : 'Parado'}
            </div>
          </div>
        </div>
        <div className="flex items-center gap-4 text-xs">
          <div className="text-right">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Reciclados</div>
            <div className="font-bold text-emerald-600">{totalReciclados}</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Rejeitados</div>
            <div className="font-bold text-red-600">{estado.rejeitadosIA}</div>
          </div>
          <div className="text-right">
            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">Descartados</div>
            <div className="font-bold text-slate-600">{estado.descartados}</div>
          </div>
        </div>
      </div>

      {/* Progresso */}
      <div className="space-y-1">
        <div className="flex items-center justify-between text-[11px] text-muted-foreground">
          <span>
            {estado.totalAuditar > 0
              ? `${estado.auditados} / ${estado.totalAuditar} anúncios`
              : 'Nenhum lote em andamento'}
          </span>
          {totalReciclados > 0 && (
            <span>
              {estado.reciclados > 0 && `${estado.reciclados} → fila normal`}
              {estado.reciclados > 0 && estado.recicladosIA > 0 && ' · '}
              {estado.recicladosIA > 0 && `${estado.recicladosIA} → fila IA`}
            </span>
          )}
        </div>
        <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
          {pct !== null ? (
            <div
              className="h-full bg-amber-500 transition-all"
              style={{ width: `${pct}%` }}
            />
          ) : ativo ? (
            <div className="h-full bg-gradient-to-r from-amber-200 via-amber-500 to-amber-200 bg-[length:200%_100%] animate-[shimmer_1.5s_infinite]" />
          ) : (
            <div className="h-full bg-transparent" />
          )}
        </div>
      </div>
    </div>
  );
}

function CardsTest({ estado, rodando }: { estado: TestState; rodando: boolean }) {
  const concluido = estado.validos !== null && estado.fila !== null && estado.erros !== null;
  const aguardando = rodando && !concluido;

  const fmt = (n: number | null): string =>
    n === null ? '—' : n.toLocaleString('pt-BR');

  type Card = {
    label: string;
    valor: number | null;
    icone: typeof Database;
    cor: string;
    bg: string;
    ring: string;
  };

  const cards: Card[] = [
    {
      label: 'Anúncios válidos',
      valor: estado.validos,
      icone: CheckCircle2,
      cor: 'text-emerald-600',
      bg: 'bg-emerald-50',
      ring: 'border-emerald-200',
    },
    {
      label: 'Total na fila',
      valor: estado.fila,
      icone: Boxes,
      cor: 'text-sky-600',
      bg: 'bg-sky-50',
      ring: 'border-sky-200',
    },
    {
      label: 'Com erros',
      valor: estado.erros,
      icone: AlertTriangle,
      cor: 'text-amber-600',
      bg: 'bg-amber-50',
      ring: 'border-amber-200',
    },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      {cards.map((c) => {
        const Icone = c.icone;
        const vazio = c.valor === null;
        return (
          <div
            key={c.label}
            className={`bg-white border rounded-xl p-4 shadow-sm transition ${
              vazio ? 'border-border' : c.ring
            }`}
          >
            <div className="flex items-center justify-between">
              <div>
                <div className="text-[11px] uppercase tracking-wide text-muted-foreground font-bold">
                  {c.label}
                </div>
                <div className={`text-2xl font-bold mt-1 ${vazio ? 'text-slate-400' : c.cor}`}>
                  {aguardando && vazio ? (
                    <Loader2 size={22} className="animate-spin" />
                  ) : (
                    fmt(c.valor)
                  )}
                </div>
              </div>
              <div className={`p-2.5 rounded-lg ${c.bg} ${c.cor}`}>
                <Icone size={20} />
              </div>
            </div>
          </div>
        );
      })}
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
          const isSpecial = ev.status === 'lfp' || ev.status === 'aci' || ev.status === 'system';
          const def = isSpecial ? null : STATUS[ev.status as StatusWorker];
          const isSystem = ev.status === 'system';
          const isAci = ev.status === 'aci';
          const Icone = isSystem ? Database : isAci ? ShieldCheck : (def?.icone ?? Radar);
          const cor = isSystem ? 'text-slate-600' : isAci ? 'text-amber-600' : (def?.cor ?? 'text-sky-600');
          const bg = isSystem ? 'bg-slate-100' : isAci ? 'bg-amber-50' : (def?.bg ?? 'bg-sky-50');
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
                  {isAci && <span className="font-bold text-amber-700 mr-1.5">Auditor</span>}
                  {isSystem && <span className="font-bold text-slate-700 mr-1.5">Sistema</span>}
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
