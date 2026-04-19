import { useEffect, useMemo, useState } from 'react';
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
import { AlertTriangle, BarChart3, MapPin, RefreshCw, TrendingUp } from 'lucide-react';
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
  coef_dispersao_pct: number;
  lat: number;
  lon: number;
}

const formatBRL = (val: number | null | undefined) =>
  val == null
    ? '—'
    : new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL', maximumFractionDigits: 0 }).format(val);

const inputClass =
  'w-full border border-border rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-primary';

// Ordena categorias de tamanho do maior pro menor.
// Usa número extraido do nome (ex: "50+ ha", "20-50 ha") e fallback por nome INCRA.
function ordenarCategoriasDoMaiorAoMenor(cats: string[]): string[] {
  const numeroMax = (s: string): number => {
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
  const [selCategoria, setSelCategoria] = useState<string | null>(null);
  const [selMunicipio, setSelMunicipio] = useState('Todos');

  const categoriasOrdenadas = useMemo(
    () => ordenarCategoriasDoMaiorAoMenor(Array.from(new Set(dadosBase.map((d) => d.categoria_tamanho)))),
    [dadosBase],
  );

  useEffect(() => {
    if (selCategoria === null && categoriasOrdenadas.length > 0) {
      setSelCategoria(categoriasOrdenadas[0]);
    }
  }, [categoriasOrdenadas, selCategoria]);

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

  const estadosDisponiveis = useMemo(() => {
    const base = selRegiao === 'Todas' ? dadosBase : dadosBase.filter((d) => d.regiao === selRegiao);
    return Array.from(new Set(base.map((d) => d.estado))).sort();
  }, [dadosBase, selRegiao]);

  const municipiosDisponiveis = useMemo(() => {
    let base = dadosBase;
    if (selRegiao !== 'Todas') base = base.filter((d) => d.regiao === selRegiao);
    if (selEstado !== 'Todos') base = base.filter((d) => d.estado === selEstado);
    const totais = new Map<string, number>();
    for (const d of base) {
      totais.set(d.municipio, (totais.get(d.municipio) ?? 0) + d.total_anuncios_reais);
    }
    return Array.from(totais.entries())
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0], 'pt-BR'))
      .map(([nome, total]) => ({ nome, total }));
  }, [dadosBase, selRegiao, selEstado]);

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
        if (selCategoria && selCategoria !== 'Todos' && d.categoria_tamanho !== selCategoria) return false;
        if (selMunicipio !== 'Todos' && d.municipio !== selMunicipio) return false;
        return true;
      }),
    [dadosBase, selRegiao, selEstado, selCategoria, selMunicipio],
  );

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
    const media = dadosFiltrados.reduce((acc, c) => acc + c.media_geral, 0) / dadosFiltrados.length;
    const dispersao =
      dadosFiltrados.reduce((acc, c) => acc + (c.coef_dispersao_pct || 0), 0) /
      dadosFiltrados.length;
    const valores = dadosFiltrados.map((d) => d.mediana_geral).sort((a, b) => a - b);
    const mid = Math.floor(valores.length / 2);
    const mediana =
      valores.length % 2 !== 0 ? valores[mid] : (valores[mid - 1] + valores[mid]) / 2;
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

        <div className="bg-white p-6 rounded-xl border border-border shadow-sm grid grid-cols-1 md:grid-cols-4 gap-4">
          <div>
            <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">
              Tamanho da Propriedade
            </label>
            <select
              value={selCategoria ?? 'Todos'}
              onChange={(e) => setSelCategoria(e.target.value)}
              className={inputClass}
            >
              <option value="Todos">Todos</option>
              {categoriasOrdenadas.map((cat) => (
                <option key={cat} value={cat}>
                  {cat}
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
            >
              <option value="Todas">Todas</option>
              <option value="Norte">Norte</option>
              <option value="Nordeste">Nordeste</option>
              <option value="Centro-Oeste">Centro-Oeste</option>
              <option value="Sudeste">Sudeste</option>
              <option value="Sul">Sul</option>
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
              onChange={(e) => setSelMunicipio(e.target.value)}
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
              {[
                {
                  t: 'Mediana (R$/ha)',
                  v: formatBRL(kpis.mediana),
                  i: BarChart3,
                  c: 'bg-white border-l-4 border-l-primary',
                },
                {
                  t: 'Preço Médio (R$/ha)',
                  v: formatBRL(kpis.media),
                  i: TrendingUp,
                  c: 'bg-accent border',
                },
                {
                  t: 'Volume Amostral',
                  v: kpis.amostras.toLocaleString('pt-BR'),
                  i: MapPin,
                  c: 'bg-white border',
                },
                {
                  t: 'Dispersão Média',
                  v: `${kpis.dispersao.toFixed(1)}%`,
                  i: BarChart3,
                  c: 'bg-[#fffaeb] border border-[#fef0c7]',
                },
              ].map((k, i) => (
                <div
                  key={i}
                  className={`${k.c} p-6 rounded-xl shadow-sm border-border flex justify-between`}
                >
                  <div>
                    <p className="text-xs font-bold text-muted-foreground uppercase">{k.t}</p>
                    <p className="text-2xl font-black mt-2 text-foreground">{k.v}</p>
                  </div>
                  <k.i size={32} className="text-primary opacity-20" />
                </div>
              ))}
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
                          {formatBRL(d.mediana_geral)}
                        </LeafletTooltip>
                      </CircleMarker>
                    ))}
                  </MapContainer>
                </div>
              </div>

              <div className="bg-white rounded-xl border border-border shadow-sm flex flex-col lg:col-span-2 overflow-hidden">
                <div className="p-4 border-b border-border font-bold">
                  Dados Detalhados ({dadosFiltrados.length})
                </div>
                <div className="flex-1 overflow-auto">
                  <table className="w-full text-sm text-left">
                    <thead className="bg-muted sticky top-0 text-xs uppercase font-bold text-muted-foreground shadow-sm">
                      <tr>
                        <th className="px-3 py-3">Município</th>
                        <th className="px-3 py-3">UF</th>
                        <th className="px-3 py-3">Categoria</th>
                        <th className="px-3 py-3 text-right">Geral</th>
                        <th className="px-3 py-3 text-right">Agrícola</th>
                        <th className="px-3 py-3 text-right">Pecuária</th>
                        <th className="px-3 py-3 text-right">F. Plantada</th>
                        <th className="px-3 py-3 text-right">F. Nativa</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {dadosFiltrados.slice(0, 100).map((r, i) => (
                        <tr key={i} className="hover:bg-muted/50">
                          <td className="px-3 py-3 font-medium">{r.municipio}</td>
                          <td className="px-3 py-3">{r.estado}</td>
                          <td className="px-3 py-3 text-muted-foreground">
                            {r.categoria_tamanho}
                          </td>
                          <td className="px-3 py-3 text-right text-primary font-bold">
                            {formatBRL(r.mediana_geral)}
                          </td>
                          <td className="px-3 py-3 text-right" title={`${r.n_agricola} amostras`}>
                            {formatBRL(r.mediana_agricola)}
                          </td>
                          <td className="px-3 py-3 text-right" title={`${r.n_pecuaria} amostras`}>
                            {formatBRL(r.mediana_pecuaria)}
                          </td>
                          <td
                            className="px-3 py-3 text-right"
                            title={`${r.n_floresta_plantada} amostras`}
                          >
                            {formatBRL(r.mediana_floresta_plantada)}
                          </td>
                          <td
                            className="px-3 py-3 text-right"
                            title={`${r.n_floresta_nativa} amostras`}
                          >
                            {formatBRL(r.mediana_floresta_nativa)}
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
