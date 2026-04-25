import { useEffect, useMemo, useRef, useState } from 'react';
import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Tooltip as LeafletTooltip,
  GeoJSON,
  useMap,
} from 'react-leaflet';
import L from 'leaflet';
import type { Geometry } from 'geojson';
import 'leaflet/dist/leaflet.css';
import { AlertTriangle, BarChart3, ChevronDown, FileSpreadsheet, FileText, Landmark, MapPin, RefreshCw, TrendingUp } from 'lucide-react';
import { api } from '@/lib/api';

interface FazendaData {
  municipio: string;
  estado: string;
  regiao: string;
  categoria_tamanho: string;
  total_anuncios_reais: number;
  mediana_geral: number;
  mediana_agricola: number | null;
  mediana_pecuaria: number | null;
  mediana_floresta_plantada: number | null;
  mediana_floresta_nativa: number | null;
  n_agricola: number;
  n_pecuaria: number;
  n_floresta_plantada: number;
  n_floresta_nativa: number;
  media_geral: number;
  media_agricola: number | null;
  media_pecuaria: number | null;
  media_floresta_plantada: number | null;
  media_floresta_nativa: number | null;
  coef_dispersao_pct: number;
  mercado_regional_codigo: string | null;
  mercado_regional_nome: string | null;
  lat: number;
  lon: number;
}

type Metrica = 'mediana' | 'media';


const formatBRL = (val: number | null | undefined) =>
  val == null
    ? '—'
    : new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(val);

const inputClass =
  'w-full border border-border rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-primary';


// Ordena categorias de tamanho do maior pro menor.
// Usa número extraido do nome (ex: "50+ ha", "20-50 ha") e fallback por nome INCRA.
// Agrega linhas do mesmo municipio (selecionadas em varios tamanhos) em uma
// unica linha. Usa media ponderada pelo numero de amostras:
//  - exato para "Media" (operacao linear)
//  - aproximado para "Mediana" (nao recalcula mediana real, usa peso amostral)
function agregarPorMunicipio(linhas: FazendaData[]): FazendaData[] {
  const grupos = new Map<string, FazendaData[]>();
  for (const l of linhas) {
    const k = `${l.estado}|${l.municipio}`;
    const g = grupos.get(k);
    if (g) g.push(l);
    else grupos.set(k, [l]);
  }

  const ponderada = (
    grupo: FazendaData[],
    getValor: (d: FazendaData) => number | null,
    getPeso: (d: FazendaData) => number,
  ): number | null => {
    let soma = 0;
    let pesos = 0;
    for (const d of grupo) {
      const v = getValor(d);
      const p = getPeso(d);
      if (v != null && isFinite(v) && p > 0) {
        soma += v * p;
        pesos += p;
      }
    }
    return pesos > 0 ? soma / pesos : null;
  };

  return Array.from(grupos.values()).map((grupo) => {
    const head = grupo[0];
    const pesoTotal = (d: FazendaData) => d.total_anuncios_reais;
    const total = grupo.reduce((a, b) => a + b.total_anuncios_reais, 0);
    return {
      ...head,
      total_anuncios_reais: total,
      categoria_tamanho:
        grupo.length === 1 ? head.categoria_tamanho : `${grupo.length} tamanhos`,
      mediana_geral: ponderada(grupo, (d) => d.mediana_geral, pesoTotal) ?? 0,
      media_geral: ponderada(grupo, (d) => d.media_geral, pesoTotal) ?? 0,
      mediana_agricola: ponderada(grupo, (d) => d.mediana_agricola, (d) => d.n_agricola),
      media_agricola: ponderada(grupo, (d) => d.media_agricola, (d) => d.n_agricola),
      mediana_pecuaria: ponderada(grupo, (d) => d.mediana_pecuaria, (d) => d.n_pecuaria),
      media_pecuaria: ponderada(grupo, (d) => d.media_pecuaria, (d) => d.n_pecuaria),
      mediana_floresta_plantada: ponderada(
        grupo, (d) => d.mediana_floresta_plantada, (d) => d.n_floresta_plantada,
      ),
      media_floresta_plantada: ponderada(
        grupo, (d) => d.media_floresta_plantada, (d) => d.n_floresta_plantada,
      ),
      mediana_floresta_nativa: ponderada(
        grupo, (d) => d.mediana_floresta_nativa, (d) => d.n_floresta_nativa,
      ),
      media_floresta_nativa: ponderada(
        grupo, (d) => d.media_floresta_nativa, (d) => d.n_floresta_nativa,
      ),
      n_agricola: grupo.reduce((a, b) => a + b.n_agricola, 0),
      n_pecuaria: grupo.reduce((a, b) => a + b.n_pecuaria, 0),
      n_floresta_plantada: grupo.reduce((a, b) => a + b.n_floresta_plantada, 0),
      n_floresta_nativa: grupo.reduce((a, b) => a + b.n_floresta_nativa, 0),
      coef_dispersao_pct: ponderada(grupo, (d) => d.coef_dispersao_pct, pesoTotal) ?? 0,
    };
  });
}

function ordenarCategoriasDoMaiorAoMenor(cats: string[]): string[] {
  const numeroMax = (s: string): number => {
    if (/^\s*mais de/i.test(s)) return Infinity;
    const ms = [...s.matchAll(/\d+(?:[.,]\d+)?/g)].map((m) => parseFloat(m[0].replace(',', '.')));
    return ms.length ? Math.max(...ms) : NaN;
  };
  const chaveTextual = (s: string) =>
    s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z]/g, '');
  const ordemINCRA: Record<string, number> = {
    grande: 4,
    gp: 4,
    media: 3,
    mediapropriedade: 3,
    pequena: 2,
    pequenapropriedade: 2,
    minifundio: 1,
  };
  return [...cats].sort((a, b) => {
    const na = numeroMax(a);
    const nb = numeroMax(b);
    if (!isNaN(na) && !isNaN(nb)) return nb - na;
    const ta = ordemINCRA[chaveTextual(a)] ?? 0;
    const tb = ordemINCRA[chaveTextual(b)] ?? 0;
    if (ta || tb) return tb - ta;
    return b.localeCompare(a, 'pt-BR');
  });
}

function FitToLayer({ geom }: { geom: Geometry | null }) {
  const map = useMap();
  useEffect(() => {
    if (!geom) return;
    const layer = L.geoJSON(geom as Geometry);
    const bounds = layer.getBounds();
    if (bounds.isValid()) {
      map.fitBounds(bounds, { padding: [20, 20], maxZoom: 12 });
    }
  }, [geom, map]);
  return null;
}

function ResetView({ trigger }: { trigger: boolean }) {
  const map = useMap();
  useEffect(() => {
    if (trigger) map.setView([-15.78, -47.92], 4);
  }, [trigger, map]);
  return null;
}

export default function Observatorio() {
  const [dadosBase, setDadosBase] = useState<FazendaData[]>([]);
  const [loading, setLoading] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [erro, setErro] = useState('');

  const [selRegiao, setSelRegiao] = useState('Todas');
  const [selEstado, setSelEstado] = useState('Todos');
  const [selCategorias, setSelCategorias] = useState<string[]>([]);
  const [selMunicipio, setSelMunicipio] = useState('Todos');
  const [selMercadoRegional, setSelMercadoRegional] = useState('Todos');
  const [metrica, setMetrica] = useState<Metrica>('mediana');

  const [catOpen, setCatOpen] = useState(false);
  const catRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!catOpen) return;
    const onClick = (e: MouseEvent) => {
      if (catRef.current && !catRef.current.contains(e.target as Node)) setCatOpen(false);
    };
    document.addEventListener('mousedown', onClick);
    return () => document.removeEventListener('mousedown', onClick);
  }, [catOpen]);
  const toggleCategoria = (cat: string) => {
    setSelCategorias((prev) =>
      prev.includes(cat) ? prev.filter((c) => c !== cat) : [...prev, cat],
    );
  };
  const rotuloCategorias =
    selCategorias.length === 0
      ? 'Todos'
      : selCategorias.length === 1
        ? selCategorias[0]
        : `${selCategorias.length} selecionados`;

  // Ao trocar o municipio, preenche estado/regiao automaticamente quando ainda
  // estiverem em "Todos"/"Todas". Mantem as selecoes ja explicitadas pelo usuario.
  const handleMunicipioChange = (nome: string) => {
    setSelMunicipio(nome);
    if (nome === 'Todos') return;
    const linha = dadosBase.find((d) => d.municipio === nome);
    if (!linha) return;
    if (selEstado === 'Todos') setSelEstado(linha.estado);
    if (selRegiao === 'Todas') setSelRegiao(linha.regiao);
  };

  const getGeral = (d: FazendaData) => (metrica === 'media' ? d.media_geral : d.mediana_geral);
  const getAgricola = (d: FazendaData) => (metrica === 'media' ? d.media_agricola : d.mediana_agricola);
  const getPecuaria = (d: FazendaData) => (metrica === 'media' ? d.media_pecuaria : d.mediana_pecuaria);
  const getFlorestaPlantada = (d: FazendaData) =>
    metrica === 'media' ? d.media_floresta_plantada : d.mediana_floresta_plantada;
  const getFlorestaNativa = (d: FazendaData) =>
    metrica === 'media' ? d.media_floresta_nativa : d.mediana_floresta_nativa;
  const rotuloMetrica = metrica === 'media' ? 'Média' : 'Mediana';

  const categoriasOrdenadas = useMemo(
    () => ordenarCategoriasDoMaiorAoMenor(Array.from(new Set(dadosBase.map((d) => d.categoria_tamanho)))),
    [dadosBase],
  );

  const [geomRegiao, setGeomRegiao] = useState<Geometry | null>(null);
  const [geomEstado, setGeomEstado] = useState<Geometry | null>(null);
  const [geomMunicipio, setGeomMunicipio] = useState<Geometry | null>(null);

  const fetchDados = async () => {
    try {
      setLoading(true);
      setErro('');
      const res = await api.get<{ sucesso: boolean; dados: FazendaData[] }>('/dados');
      if (res.data.sucesso) {
        setDadosBase(res.data.dados);
        if (res.data.dados.length === 0) {
          setErro("Base de dados vazia. Clique em 'Sincronizar Base' para calcular.");
        }
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : 'Erro desconhecido ao conectar com o banco';
      setErro(msg);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchDados();
  }, []);

  const handleSincronizar = async () => {
    try {
      setSyncing(true);
      await api.post('/sincronizar');
      await fetchDados();
    } catch (e) {
      setErro('Falha ao sincronizar: ' + (e instanceof Error ? e.message : ''));
    } finally {
      setSyncing(false);
    }
  };

  const [baixando, setBaixando] = useState<'xlsx' | 'pdf' | 'grandes-xlsx' | 'grandes-pdf' | null>(null);

  const filtrosAtuais = () => ({
    regiao: selRegiao !== 'Todas' ? selRegiao : undefined,
    estado: selEstado !== 'Todos' ? selEstado : undefined,
    categoria: selCategorias.length > 0 ? selCategorias.join(',') : undefined,
    municipio: selMunicipio !== 'Todos' ? selMunicipio : undefined,
    mercado_regional: selMercadoRegional !== 'Todos' ? selMercadoRegional : undefined,
  });

  const baixarArquivo = (blob: Blob, nome: string) => {
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = nome;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
  };

  const handleExportarXLSX = async () => {
    try {
      setBaixando('xlsx');
      const res = await api.get('/relatorio/xlsx', {
        params: filtrosAtuais(),
        responseType: 'blob',
        timeout: 180000,
      });
      const ts = new Date().toISOString().slice(0, 10);
      baixarArquivo(res.data, `simet_${ts}.xlsx`);
    } catch (e) {
      setErro('Falha ao exportar XLSX: ' + (e instanceof Error ? e.message : ''));
    } finally {
      setBaixando(null);
    }
  };

  const handleExportarPDF = async () => {
    if (selEstado === 'Todos') {
      setErro('Selecione uma UF antes de exportar o PDF.');
      return;
    }
    try {
      setBaixando('pdf');
      const res = await api.get('/relatorio/pdf', {
        params: {
          estado: selEstado,
          categoria: selCategorias.length > 0 ? selCategorias.join(',') : undefined,
          mercado_regional: selMercadoRegional !== 'Todos' ? selMercadoRegional : undefined,
        },
        responseType: 'blob',
        timeout: 180000,
      });
      const ts = new Date().toISOString().slice(0, 10);
      baixarArquivo(res.data, `simet_${selEstado}_${ts}.pdf`);
    } catch (e) {
      setErro('Falha ao exportar PDF: ' + (e instanceof Error ? e.message : ''));
    } finally {
      setBaixando(null);
    }
  };

  const filtrosGrandes = () => ({
    regiao: selRegiao !== 'Todas' ? selRegiao : undefined,
    estado: selEstado !== 'Todos' ? selEstado : undefined,
    municipio: selMunicipio !== 'Todos' ? selMunicipio : undefined,
    mercado_regional: selMercadoRegional !== 'Todos' ? selMercadoRegional : undefined,
  });

  const handleExportarGrandesXLSX = async () => {
    try {
      setBaixando('grandes-xlsx');
      const res = await api.get('/relatorio/grandes-propriedades/xlsx', {
        params: filtrosGrandes(),
        responseType: 'blob',
        timeout: 180000,
      });
      const ts = new Date().toISOString().slice(0, 10);
      baixarArquivo(res.data, `simet_grandes_propriedades_${ts}.xlsx`);
    } catch (e) {
      setErro('Falha ao exportar XLSX (Grandes Propriedades): ' + (e instanceof Error ? e.message : ''));
    } finally {
      setBaixando(null);
    }
  };

  const handleExportarGrandesPDF = async () => {
    if (selEstado === 'Todos') {
      setErro('Selecione uma UF antes de exportar o PDF de Grandes Propriedades.');
      return;
    }
    try {
      setBaixando('grandes-pdf');
      const res = await api.get('/relatorio/grandes-propriedades/pdf', {
        params: {
          estado: selEstado,
          regiao: selRegiao !== 'Todas' ? selRegiao : undefined,
          municipio: selMunicipio !== 'Todos' ? selMunicipio : undefined,
          mercado_regional: selMercadoRegional !== 'Todos' ? selMercadoRegional : undefined,
        },
        responseType: 'blob',
        timeout: 180000,
      });
      const ts = new Date().toISOString().slice(0, 10);
      baixarArquivo(res.data, `simet_grandes_propriedades_${selEstado}_${ts}.pdf`);
    } catch (e) {
      setErro('Falha ao exportar PDF (Grandes Propriedades): ' + (e instanceof Error ? e.message : ''));
    } finally {
      setBaixando(null);
    }
  };

  const baseMercado = useMemo(
    () =>
      selMercadoRegional === 'Todos'
        ? dadosBase
        : dadosBase.filter((d) => d.mercado_regional_codigo === selMercadoRegional),
    [dadosBase, selMercadoRegional],
  );

  const regioesDisponiveis = useMemo(
    () => Array.from(new Set(baseMercado.map((d) => d.regiao))).sort(),
    [baseMercado],
  );

  const estadosDisponiveis = useMemo(() => {
    const base = selRegiao === 'Todas' ? baseMercado : baseMercado.filter((d) => d.regiao === selRegiao);
    return Array.from(new Set(base.map((d) => d.estado))).sort();
  }, [baseMercado, selRegiao]);

  const municipiosDisponiveis = useMemo(() => {
    let base = baseMercado;
    if (selRegiao !== 'Todas') base = base.filter((d) => d.regiao === selRegiao);
    if (selEstado !== 'Todos') base = base.filter((d) => d.estado === selEstado);
    const totais = new Map<string, number>();
    for (const d of base) {
      totais.set(d.municipio, (totais.get(d.municipio) ?? 0) + d.total_anuncios_reais);
    }
    return Array.from(totais.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], 'pt-BR'))
      .map(([nome, total]) => ({ nome, total }));
  }, [baseMercado, selRegiao, selEstado]);

  useEffect(() => {
    if (selRegiao !== 'Todas' && !regioesDisponiveis.includes(selRegiao)) setSelRegiao('Todas');
  }, [regioesDisponiveis, selRegiao]);

  useEffect(() => {
    if (selEstado !== 'Todos' && !estadosDisponiveis.includes(selEstado)) setSelEstado('Todos');
  }, [estadosDisponiveis, selEstado]);

  useEffect(() => {
    if (selMunicipio !== 'Todos' && !municipiosDisponiveis.some((m) => m.nome === selMunicipio)) {
      setSelMunicipio('Todos');
    }
  }, [municipiosDisponiveis, selMunicipio]);

  const dadosFiltrados = useMemo(
    () =>
      dadosBase.filter((d) => {
        if (selRegiao !== 'Todas' && d.regiao !== selRegiao) return false;
        if (selEstado !== 'Todos' && d.estado !== selEstado) return false;
        if (selCategorias.length > 0 && !selCategorias.includes(d.categoria_tamanho)) return false;
        if (selMunicipio !== 'Todos' && d.municipio !== selMunicipio) return false;
        if (selMercadoRegional !== 'Todos' && d.mercado_regional_codigo !== selMercadoRegional) return false;
        return true;
      }),
    [dadosBase, selRegiao, selEstado, selCategorias, selMunicipio, selMercadoRegional],
  );

  // Quando 2+ tamanhos estao marcados, consolida as linhas do mesmo municipio
  // em uma unica linha (ponderada pelo volume amostral)
  const linhasTabela = useMemo(
    () => (selCategorias.length >= 2 ? agregarPorMunicipio(dadosFiltrados) : dadosFiltrados),
    [dadosFiltrados, selCategorias],
  );

  const mercadosDisponiveis = useMemo(() => {
    let base = dadosBase;
    if (selRegiao !== 'Todas') base = base.filter((d) => d.regiao === selRegiao);
    if (selEstado !== 'Todos') base = base.filter((d) => d.estado === selEstado);
    if (selMunicipio !== 'Todos') base = base.filter((d) => d.municipio === selMunicipio);
    const porCodigo = new Map<string, { codigo: string; nome: string; ufs: Set<string> }>();
    for (const d of base) {
      if (!d.mercado_regional_codigo || !d.mercado_regional_nome) continue;
      const cur = porCodigo.get(d.mercado_regional_codigo);
      if (cur) {
        cur.ufs.add(d.estado);
      } else {
        porCodigo.set(d.mercado_regional_codigo, {
          codigo: d.mercado_regional_codigo,
          nome: d.mercado_regional_nome,
          ufs: new Set([d.estado]),
        });
      }
    }
    return Array.from(porCodigo.values())
      .map((m) => ({ codigo: m.codigo, nome: m.nome, ufs: Array.from(m.ufs).sort() }))
      .sort((a, b) => a.nome.localeCompare(b.nome, 'pt-BR'));
  }, [dadosBase, selRegiao, selEstado, selMunicipio]);

  useEffect(() => {
    if (selMercadoRegional !== 'Todos' && !mercadosDisponiveis.some((m) => m.codigo === selMercadoRegional)) {
      setSelMercadoRegional('Todos');
    }
  }, [mercadosDisponiveis, selMercadoRegional]);

  useEffect(() => {
    setGeomRegiao(null);
    if (selRegiao === 'Todas') return;
    let cancelled = false;
    api
      .get<{ sucesso: boolean; geom: Geometry | null }>('/geom', {
        params: { tipo: 'regiao', nome: selRegiao },
      })
      .then((res) => !cancelled && setGeomRegiao(res.data.geom))
      .catch(() => !cancelled && setGeomRegiao(null));
    return () => {
      cancelled = true;
    };
  }, [selRegiao]);

  useEffect(() => {
    setGeomEstado(null);
    if (selEstado === 'Todos') return;
    let cancelled = false;
    api
      .get<{ sucesso: boolean; geom: Geometry | null }>('/geom', {
        params: { tipo: 'estado', uf: selEstado },
      })
      .then((res) => !cancelled && setGeomEstado(res.data.geom))
      .catch(() => !cancelled && setGeomEstado(null));
    return () => {
      cancelled = true;
    };
  }, [selEstado]);

  useEffect(() => {
    setGeomMunicipio(null);
    if (selMunicipio === 'Todos' || selEstado === 'Todos') return;
    let cancelled = false;
    api
      .get<{ sucesso: boolean; geom: Geometry | null }>('/geom', {
        params: { tipo: 'municipio', nome: selMunicipio, uf: selEstado },
      })
      .then((res) => !cancelled && setGeomMunicipio(res.data.geom))
      .catch(() => !cancelled && setGeomMunicipio(null));
    return () => {
      cancelled = true;
    };
  }, [selMunicipio, selEstado]);

  const kpis = useMemo(() => {
    if (dadosFiltrados.length === 0) return { mediana: 0, media: 0, amostras: 0, dispersao: 0 };
    const amostras = dadosFiltrados.reduce((acc, c) => acc + c.total_anuncios_reais, 0);

    // Todos os agregados abaixo sao ponderados pelo volume amostral, alinhando o
    // calculo dos KPIs com a agregacao da tabela detalhada (agregarPorMunicipio).
    let somaMediana = 0;
    let somaMedia = 0;
    let somaDispersao = 0;
    let pesos = 0;
    for (const d of dadosFiltrados) {
      const p = d.total_anuncios_reais;
      if (p <= 0) continue;
      if (d.mediana_geral != null && isFinite(d.mediana_geral)) somaMediana += d.mediana_geral * p;
      if (d.media_geral != null && isFinite(d.media_geral)) somaMedia += d.media_geral * p;
      if (d.coef_dispersao_pct != null && isFinite(d.coef_dispersao_pct))
        somaDispersao += d.coef_dispersao_pct * p;
      pesos += p;
    }
    const mediana = pesos > 0 ? somaMediana / pesos : 0;
    const media = pesos > 0 ? somaMedia / pesos : 0;
    const dispersao = pesos > 0 ? somaDispersao / pesos : 0;

    return { mediana, media, amostras, dispersao };
  }, [dadosFiltrados]);

  return (
    <>
      <header className="bg-white border-b border-border px-8 py-6 flex justify-between items-center shrink-0 z-10">
        <div>
          <h1 className="text-2xl font-bold">Observatório de Mercado</h1>
          <p className="text-muted-foreground text-sm">
            Visualização geoespacial e métricas financeiras
          </p>
        </div>
        <button
          onClick={handleSincronizar}
          disabled={syncing}
          className="bg-white border border-border text-foreground px-4 py-2 rounded-md font-medium flex items-center gap-2 hover:bg-muted transition shadow-sm disabled:opacity-60"
        >
          <RefreshCw size={16} className={syncing ? 'animate-spin' : ''} />
          {syncing ? 'Sincronizando...' : 'Sincronizar Base'}
        </button>
      </header>

      <div className="flex-1 overflow-auto p-8 space-y-6 relative z-0">
        {erro && (
          <div className="bg-amber-50 border border-amber-200 text-amber-800 p-4 rounded-xl flex items-start gap-3">
            <AlertTriangle className="shrink-0 mt-0.5" />
            <div>
              <h3 className="font-bold">Aviso do Sistema</h3>
              <p className="text-sm">{erro}</p>
            </div>
          </div>
        )}

        <div className="bg-white p-6 rounded-xl border border-border shadow-sm grid grid-cols-1 md:grid-cols-3 xl:grid-cols-5 gap-4">
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Tamanho da Propriedade
            </label>
            <div ref={catRef} className="relative">
              <button
                type="button"
                onClick={() => setCatOpen((v) => !v)}
                className={`${inputClass} text-left flex justify-between items-center`}
                aria-haspopup="listbox"
                aria-expanded={catOpen}
              >
                <span className="truncate">{rotuloCategorias}</span>
                <ChevronDown
                  size={16}
                  className={`shrink-0 ml-2 transition-transform ${catOpen ? 'rotate-180' : ''}`}
                />
              </button>
              {catOpen && (
                <div
                  role="listbox"
                  className="absolute z-30 mt-1 w-full bg-white border border-border rounded-md shadow-lg max-h-64 overflow-auto"
                >
                  <button
                    type="button"
                    onClick={() => setSelCategorias([])}
                    className="w-full text-left px-3 py-2 hover:bg-muted text-sm border-b border-border font-medium"
                  >
                    Todos (limpar seleção)
                  </button>
                  {categoriasOrdenadas.map((cat) => {
                    const marcado = selCategorias.includes(cat);
                    return (
                      <label
                        key={cat}
                        className="flex items-center gap-2 px-3 py-2 hover:bg-muted cursor-pointer text-sm"
                      >
                        <input
                          type="checkbox"
                          checked={marcado}
                          onChange={() => toggleCategoria(cat)}
                          className="accent-primary"
                        />
                        <span>{cat}</span>
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          </div>
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Mercado Regional
            </label>
            <select
              value={selMercadoRegional}
              onChange={(e) => setSelMercadoRegional(e.target.value)}
              className={inputClass}
              disabled={mercadosDisponiveis.length === 0}
            >
              <option value="Todos">Todos</option>
              {mercadosDisponiveis.map((m) => (
                <option key={m.codigo} value={m.codigo}>
                  {m.ufs.length > 0 ? `${m.ufs.join('/')} · ` : ''}{m.nome}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Região
            </label>
            <select
              value={selRegiao}
              onChange={(e) => setSelRegiao(e.target.value)}
              className={inputClass}
              disabled={regioesDisponiveis.length === 0}
            >
              <option value="Todas">Todas</option>
              {regioesDisponiveis.map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Estado
            </label>
            <select
              value={selEstado}
              onChange={(e) => setSelEstado(e.target.value)}
              className={inputClass}
            >
              <option value="Todos">Todos</option>
              {estadosDisponiveis.map((uf) => (
                <option key={uf} value={uf}>
                  {uf}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Município
            </label>
            <select
              value={selMunicipio}
              onChange={(e) => handleMunicipioChange(e.target.value)}
              className={inputClass}
              disabled={municipiosDisponiveis.length === 0}
            >
              <option value="Todos">Todos</option>
              {municipiosDisponiveis.map((m) => (
                <option key={m.nome} value={m.nome}>
                  {m.nome} ({m.total})
                </option>
              ))}
            </select>
          </div>
        </div>

        {loading ? (
          <div className="text-center p-10 font-medium text-muted-foreground animate-pulse">
            Carregando base de dados...
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
              {([
                { id: 'mediana', t: 'Mediana (R$/ha)', v: formatBRL(kpis.mediana), i: BarChart3 },
                { id: 'media',   t: 'Média (R$/ha)',   v: formatBRL(kpis.media),   i: TrendingUp },
              ] as const).map((k) => {
                const ativo = metrica === k.id;
                return (
                  <button
                    key={k.id}
                    type="button"
                    onClick={() => setMetrica(k.id)}
                    aria-pressed={ativo}
                    title={ativo ? `${k.t} (selecionada)` : `Usar ${k.t} no detalhamento`}
                    className={`p-6 rounded-xl shadow-sm flex justify-between text-left transition ${
                      ativo
                        ? 'bg-white border-l-4 border-l-primary ring-2 ring-primary/30'
                        : 'bg-white border border-border hover:border-primary/40 hover:bg-muted/50 opacity-80'
                    }`}
                  >
                    <div>
                      <p className="text-xs font-bold text-muted-foreground uppercase">{k.t}</p>
                      <p className="text-2xl font-black mt-2 text-foreground">{k.v}</p>
                    </div>
                    <k.i size={32} className="text-primary opacity-20" />
                  </button>
                );
              })}
              <div className="bg-white border border-border p-6 rounded-xl shadow-sm flex justify-between">
                <div>
                  <p className="text-xs font-bold text-muted-foreground uppercase">Volume Amostral</p>
                  <p className="text-2xl font-black mt-2 text-foreground">
                    {kpis.amostras.toLocaleString('pt-BR')}
                  </p>
                </div>
                <MapPin size={32} className="text-primary opacity-20" />
              </div>
              <div className="bg-[#fffaeb] border border-[#fef0c7] p-6 rounded-xl shadow-sm flex justify-between">
                <div>
                  <p className="text-xs font-bold text-muted-foreground uppercase">Dispersão Média</p>
                  <p className="text-2xl font-black mt-2 text-foreground">
                    {kpis.dispersao.toFixed(1)}%
                  </p>
                </div>
                <BarChart3 size={32} className="text-primary opacity-20" />
              </div>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[500px]">
              <div className="bg-white rounded-xl border border-border shadow-sm overflow-hidden flex flex-col relative z-0">
                <div className="p-4 border-b border-border font-bold">Mapa Geográfico</div>
                <div className="flex-1 relative z-0">
                  <MapContainer
                    center={[-15.78, -47.92]}
                    zoom={4}
                    style={{ height: '100%', width: '100%', zIndex: 0 }}
                  >
                    <TileLayer url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png" />

                    <ResetView trigger={selRegiao === 'Todas' && selEstado === 'Todos' && selMunicipio === 'Todos'} />

                    {geomRegiao && (
                      <GeoJSON
                        key={`regiao-${selRegiao}`}
                        data={geomRegiao as never}
                        style={{ color: '#f59e0b', weight: 2, fillColor: '#fde047', fillOpacity: 0.25 }}
                      />
                    )}
                    {geomEstado && (
                      <GeoJSON
                        key={`estado-${selEstado}`}
                        data={geomEstado as never}
                        style={{ color: '#2563eb', weight: 2, fillColor: '#60a5fa', fillOpacity: 0.3 }}
                      />
                    )}
                    {geomMunicipio && (
                      <GeoJSON
                        key={`mun-${selMunicipio}-${selEstado}`}
                        data={geomMunicipio as never}
                        style={{ color: '#16a34a', weight: 2, fillColor: '#86efac', fillOpacity: 0.55 }}
                      />
                    )}

                    <FitToLayer
                      geom={geomMunicipio ?? geomEstado ?? geomRegiao}
                    />

                    {dadosFiltrados.slice(0, 1000).map((d, i) => (
                      <CircleMarker
                        key={i}
                        center={[d.lat, d.lon]}
                        radius={6}
                        pathOptions={{ fillColor: '#005826', color: '#005826', fillOpacity: 0.7 }}
                      >
                        <LeafletTooltip>
                          <b>
                            {d.municipio} - {d.estado}
                          </b>
                          <br />
                          {rotuloMetrica}: {formatBRL(getGeral(d))}
                          {d.mercado_regional_nome && (
                            <>
                              <br />
                              <span style={{ fontSize: 10, opacity: 0.7 }}>
                                {d.mercado_regional_nome}
                              </span>
                            </>
                          )}
                        </LeafletTooltip>
                      </CircleMarker>
                    ))}
                  </MapContainer>
                </div>
              </div>

              <div className="bg-white rounded-xl border border-border shadow-sm flex flex-col lg:col-span-2 overflow-hidden">
                <div className="p-4 border-b border-border font-bold flex items-center justify-between gap-3">
                  <span>Dados Detalhados ({linhasTabela.length})</span>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={handleExportarXLSX}
                      disabled={baixando !== null}
                      title="Exportar XLSX (todos os registros filtrados)"
                      className="p-2 rounded-md border border-border hover:bg-emerald-50 hover:border-emerald-300 transition disabled:opacity-50"
                    >
                      <FileSpreadsheet size={18} className="text-emerald-700" />
                    </button>
                    <button
                      onClick={handleExportarPDF}
                      disabled={baixando !== null || selEstado === 'Todos'}
                      title={
                        selEstado === 'Todos'
                          ? 'Selecione uma UF para gerar o PDF'
                          : `Exportar PDF da UF ${selEstado}`
                      }
                      className="p-2 rounded-md border border-border hover:bg-red-50 hover:border-red-300 transition disabled:opacity-50 disabled:cursor-not-allowed"
                    >
                      <FileText size={18} className="text-red-700" />
                    </button>
                    <span className="mx-1 h-6 w-px bg-border" aria-hidden />
                    <button
                      onClick={handleExportarGrandesXLSX}
                      disabled={baixando !== null}
                      title="Relatório de Grandes Propriedades · XLSX (fazendas ≥ 50 ha, layout INCRA/UFF)"
                      className="p-2 rounded-md border border-border hover:bg-amber-50 hover:border-amber-300 transition disabled:opacity-50 flex items-center gap-1"
                    >
                      <Landmark size={18} className="text-amber-700" />
                      <FileSpreadsheet size={14} className="text-amber-700" />
                    </button>
                    <button
                      onClick={handleExportarGrandesPDF}
                      disabled={baixando !== null || selEstado === 'Todos'}
                      title={
                        selEstado === 'Todos'
                          ? 'Selecione uma UF para gerar o PDF de Grandes Propriedades'
                          : `Relatório de Grandes Propriedades · PDF da UF ${selEstado} (fazendas ≥ 50 ha)`
                      }
                      className="p-2 rounded-md border border-border hover:bg-amber-50 hover:border-amber-300 transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1"
                    >
                      <Landmark size={18} className="text-amber-700" />
                      <FileText size={14} className="text-amber-700" />
                    </button>
                  </div>
                </div>
                <div className="flex-1 overflow-auto">
                  <table className="w-full text-sm text-left">
                    <thead className="bg-muted sticky top-0 text-xs uppercase font-bold text-muted-foreground shadow-sm">
                      <tr>
                        <th className="px-3 py-3">Município</th>
                        <th className="px-3 py-3">UF</th>
                        <th className="px-3 py-3">Tamanho</th>
                        <th className="px-3 py-3 text-right">
                          <div>{rotuloMetrica} Geral</div>
                          <div className="text-[10px] font-normal normal-case text-muted-foreground/80">R$/ha</div>
                        </th>
                        <th className="px-3 py-3 text-right">
                          <div>Agrícola</div>
                          <div className="text-[10px] font-normal normal-case text-muted-foreground/80">R$/ha</div>
                        </th>
                        <th className="px-3 py-3 text-right">
                          <div>Pecuária</div>
                          <div className="text-[10px] font-normal normal-case text-muted-foreground/80">R$/ha</div>
                        </th>
                        <th className="px-3 py-3 text-right">
                          <div>Floresta Plantada</div>
                          <div className="text-[10px] font-normal normal-case text-muted-foreground/80">R$/ha</div>
                        </th>
                        <th className="px-3 py-3 text-right">
                          <div>Floresta Nativa</div>
                          <div className="text-[10px] font-normal normal-case text-muted-foreground/80">R$/ha</div>
                        </th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {linhasTabela.slice(0, 100).map((r, i) => (
                        <tr key={i} className="hover:bg-muted/50">
                          <td className="px-3 py-3 font-medium">{r.municipio}</td>
                          <td className="px-3 py-3">{r.estado}</td>
                          <td className="px-3 py-3 text-muted-foreground">
                            {r.categoria_tamanho}
                          </td>
                          <td className="px-3 py-3 text-right text-primary font-bold">
                            {formatBRL(getGeral(r))}
                          </td>
                          <td className="px-3 py-3 text-right" title={`${r.n_agricola} amostras`}>
                            {formatBRL(getAgricola(r))}
                          </td>
                          <td className="px-3 py-3 text-right" title={`${r.n_pecuaria} amostras`}>
                            {formatBRL(getPecuaria(r))}
                          </td>
                          <td
                            className="px-3 py-3 text-right"
                            title={`${r.n_floresta_plantada} amostras`}
                          >
                            {formatBRL(getFlorestaPlantada(r))}
                          </td>
                          <td
                            className="px-3 py-3 text-right"
                            title={`${r.n_floresta_nativa} amostras`}
                          >
                            {formatBRL(getFlorestaNativa(r))}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </>
  );
}
