import { useState, useEffect, useMemo } from "react";
import axios from "axios";
import { MapContainer, TileLayer, CircleMarker, Tooltip as LeafletTooltip } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import { MapPin, TrendingUp, BarChart3, RefreshCw, Menu, X, Lock, User, LogOut, Cpu, TerminalSquare, AlertTriangle } from "lucide-react";

interface FazendaData {
  municipio: string; estado: string; regiao: string; categoria_tamanho: string;
  total_anuncios_reais: number; mediana_geral: number; media_geral: number;
  coef_dispersao_pct: number; lat: number; lon: number;
}

export default function App() {
  // =========================================================
  // SISTEMA DE LOGIN & AUTENTICAÇÃO
  // =========================================================
  const [token, setToken] = useState<string | null>(localStorage.getItem("simet_token"));
  const [usuario, setUsuario] = useState("");
  const [senha, setSenha] = useState("");
  const [erroLogin, setErroLogin] = useState("");
  const [fazendoLogin, setFazendoLogin] = useState(false);

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setErroLogin(""); setFazendoLogin(true);
    try {
      const res = await axios.post("/api/login", { usuario, senha });
      if (res.data.sucesso) {
        localStorage.setItem("simet_token", res.data.token);
        setToken(res.data.token);
      }
    } catch (err) {
      setErroLogin("Usuário ou senha incorretos.");
    } finally {
      setFazendoLogin(false);
    }
  };

  const handleLogout = () => { localStorage.removeItem("simet_token"); setToken(null); };

  // =========================================================
  // NAVEGAÇÃO LATERAL
  // =========================================================
  const [abaAtiva, setAbaAtiva] = useState<"observatorio" | "comando">("observatorio");
  const [sidebarOpen, setSidebarOpen] = useState(true);

  // =========================================================
  // ESTADOS DO OBSERVATÓRIO DE TERRAS
  // =========================================================
  const [dadosBase, setDadosBase] = useState<FazendaData[]>([]);
  const [loadingDados, setLoadingDados] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [erroDados, setErroDados] = useState(""); // <-- NOVO: Para diagnosticar o erro

  const [selRegiao, setSelRegiao] = useState("Todas");
  const [selEstado, setSelEstado] = useState("Todos");
  const [selCategoria, setSelCategoria] = useState("Todos");
  const [buscaMunicipio, setBuscaMunicipio] = useState("");

  const fetchDados = async () => {
    try {
      setLoadingDados(true);
      setErroDados("");
      const res = await axios.get("/api/dados");
      if (res.data.sucesso) {
        setDadosBase(res.data.dados);
        if (res.data.dados.length === 0) setErroDados("A base de dados (View) retornou vazia. Tente clicar em 'Sincronizar Base'.");
      }
    } catch (error: any) {
      console.error("Erro na API:", error);
      setErroDados(error.message || "Erro desconhecido ao conectar com o banco de dados.");
    } finally {
      setLoadingDados(false);
    }
  };

  useEffect(() => { if (token && abaAtiva === "observatorio") fetchDados(); }, [token, abaAtiva]);

  const handleSincronizar = async () => {
    try {
      setSyncing(true);
      await axios.post("/api/sincronizar");
      await fetchDados();
    } catch (error: any) {
      setErroDados("Falha ao sincronizar: " + (error.message || ""));
    } finally {
      setSyncing(false);
    }
  };

  const dadosFiltrados = useMemo(() => {
    return dadosBase.filter(d => {
      const matchRegiao = selRegiao === "Todas" || d.regiao === selRegiao;
      const matchEstado = selEstado === "Todos" || d.estado === selEstado;
      const matchCat = selCategoria === "Todos" || d.categoria_tamanho === selCategoria;
      const matchMun = buscaMunicipio === "" || d.municipio.toLowerCase().includes(buscaMunicipio.toLowerCase());
      return matchRegiao && matchEstado && matchCat && matchMun;
    });
  }, [dadosBase, selRegiao, selEstado, selCategoria, buscaMunicipio]);

  const kpis = useMemo(() => {
    if (dadosFiltrados.length === 0) return { mediana: 0, media: 0, amostras: 0, dispersao: 0 };
    const amostras = dadosFiltrados.reduce((acc, curr) => acc + curr.total_anuncios_reais, 0);
    const media = dadosFiltrados.reduce((acc, curr) => acc + curr.media_geral, 0) / dadosFiltrados.length;
    const dispersao = dadosFiltrados.reduce((acc, curr) => acc + (curr.coef_dispersao_pct || 0), 0) / dadosFiltrados.length;
    const valores = dadosFiltrados.map(d => d.mediana_geral).sort((a, b) => a - b);
    const mid = Math.floor(valores.length / 2);
    const mediana = valores.length % 2 !== 0 ? valores[mid] : (valores[mid - 1] + valores[mid]) / 2;
    return { mediana, media, amostras, dispersao };
  }, [dadosFiltrados]);

  // =========================================================
  // ESTADOS DA CENTRAL DE COMANDO (ROBÔS)
  // =========================================================
  const [roboRodando, setRoboRodando] = useState(false);
  const [logsRobo, setLogsRobo] = useState<string[]>([]);
  const [selOperacao, setSelOperacao] = useState("full");
  const [selWorkers, setSelWorkers] = useState("3");

  // Consulta o status do robô no backend a cada 2 segundos
  useEffect(() => {
    if (!token || abaAtiva !== "comando") return;
    const interval = setInterval(async () => {
      try {
        const res = await axios.get("/api/robo/status");
        setRoboRodando(res.data.rodando);
        setLogsRobo(res.data.logs_recentes || []);
      } catch (e) { /* Ignora erros de rede no polling */ }
    }, 2000);
    return () => clearInterval(interval);
  }, [token, abaAtiva]);

  const handleIniciarRobo = async () => {
    try { await axios.post("/api/robo/iniciar", { task: selOperacao, workers: parseInt(selWorkers) }); } 
    catch (e) { console.error("Erro ao iniciar robô", e); }
  };

  const handlePararRobo = async () => {
    try { await axios.post("/api/robo/parar"); } 
    catch (e) { console.error("Erro ao parar robô", e); }
  };


  const formatBRL = (val: number) => new Intl.NumberFormat('pt-BR', { style: 'currency', currency: 'BRL' }).format(val);
  const inputClass = "w-full border border-border rounded-md px-3 py-2 text-sm bg-white focus:outline-none focus:ring-2 focus:ring-primary";

  // =========================================================
  // RENDER: TELA DE LOGIN
  // =========================================================
  if (!token) {
    return (
      <div className="flex h-screen bg-background items-center justify-center p-4">
        <div className="w-full max-w-md bg-white rounded-2xl shadow-xl border border-border p-8">
          <div className="flex flex-col items-center mb-8">
            <div className="w-16 h-16 bg-primary rounded-xl flex items-center justify-center text-white mb-4 shadow-md">
              <MapPin size={32} />
            </div>
            <h1 className="text-2xl font-bold text-foreground">SIMET - INCRA</h1>
            <p className="text-muted-foreground text-sm text-center mt-1">Plataforma Analítica de Valores Fundiários</p>
          </div>
          <form onSubmit={handleLogin} className="space-y-5">
            {erroLogin && <div className="bg-red-50 text-red-600 p-3 rounded-lg text-sm text-center border border-red-100">{erroLogin}</div>}
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Usuário</label>
              <div className="relative">
                <User size={18} className="absolute top-2.5 left-3 text-muted-foreground" />
                <input type="text" value={usuario} onChange={e => setUsuario(e.target.value)} className={`${inputClass} pl-10`} required />
              </div>
            </div>
            <div>
              <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Senha</label>
              <div className="relative">
                <Lock size={18} className="absolute top-2.5 left-3 text-muted-foreground" />
                <input type="password" value={senha} onChange={e => setSenha(e.target.value)} className={`${inputClass} pl-10`} required />
              </div>
            </div>
            <button type="submit" disabled={fazendoLogin} className="w-full bg-primary hover:bg-primary/90 text-white font-bold py-3 px-4 rounded-lg">
              {fazendoLogin ? "Autenticando..." : "Entrar no Sistema"}
            </button>
          </form>
        </div>
      </div>
    );
  }

  // =========================================================
  // RENDER: APLICAÇÃO PRINCIPAL
  // =========================================================
  return (
    <div className="flex h-screen bg-background font-sans text-foreground">
      {/* Sidebar */}
      <aside className={`${sidebarOpen ? "w-64" : "w-20"} bg-white border-r border-border transition-all duration-300 flex flex-col z-20`}>
        <div className="p-6 border-b border-border flex items-center gap-3">
          <div className="w-10 h-10 bg-primary rounded-lg flex items-center justify-center text-white shrink-0 shadow">
            <MapPin size={24} />
          </div>
          {sidebarOpen && <span className="font-bold text-lg text-primary">SIMET INCRA</span>}
        </div>
        
        <nav className="flex-1 p-4 space-y-2">
          <button 
            onClick={() => setAbaAtiva("observatorio")}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-medium transition-colors ${abaAtiva === "observatorio" ? "bg-accent text-primary" : "text-muted-foreground hover:bg-muted"}`}
          >
            <BarChart3 size={20} /> {sidebarOpen && "Observatório de Terras"}
          </button>
          <button 
            onClick={() => setAbaAtiva("comando")}
            className={`w-full flex items-center gap-3 px-4 py-3 rounded-lg font-medium transition-colors ${abaAtiva === "comando" ? "bg-accent text-primary" : "text-muted-foreground hover:bg-muted"}`}
          >
            <Cpu size={20} /> {sidebarOpen && "Central de Comando"}
          </button>
        </nav>

        <div className="p-4 border-t border-border flex flex-col gap-2">
          <button onClick={handleLogout} className="w-full flex justify-center items-center gap-2 p-2 hover:bg-red-50 text-red-600 rounded-lg transition-colors">
             <LogOut size={20} /> {sidebarOpen && <span className="text-sm font-bold">Sair</span>}
          </button>
          <button onClick={() => setSidebarOpen(!sidebarOpen)} className="w-full flex justify-center p-2 hover:bg-muted rounded-lg text-muted-foreground mt-2">
            {sidebarOpen ? <X size={20} /> : <Menu size={20} />}
          </button>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        
        {/* ========================================================= */}
        {/* ABA 1: OBSERVATÓRIO DE TERRAS                             */}
        {/* ========================================================= */}
        {abaAtiva === "observatorio" && (
          <>
            <header className="bg-white border-b border-border px-8 py-6 flex justify-between items-center shrink-0 z-10">
              <div>
                <h1 className="text-2xl font-bold">Observatório de Mercado</h1>
                <p className="text-muted-foreground text-sm">Visualização geoespacial e métricas financeiras</p>
              </div>
              <button onClick={handleSincronizar} disabled={syncing} className="bg-white border border-border text-foreground px-4 py-2 rounded-md font-medium flex items-center gap-2 hover:bg-muted transition shadow-sm">
                <RefreshCw size={16} className={syncing ? "animate-spin" : ""} /> {syncing ? "Sincronizando..." : "Sincronizar Base"}
              </button>
            </header>

            <div className="flex-1 overflow-auto p-8 space-y-6 relative z-0">
              {/* Diagnóstico de Erro */}
              {erroDados && (
                <div className="bg-amber-50 border border-amber-200 text-amber-800 p-4 rounded-xl flex items-start gap-3">
                  <AlertTriangle className="shrink-0 mt-0.5" />
                  <div>
                    <h3 className="font-bold">Aviso do Sistema</h3>
                    <p className="text-sm">{erroDados}</p>
                  </div>
                </div>
              )}

              <div className="bg-white p-6 rounded-xl border border-border shadow-sm flex flex-col md:flex-row gap-4">
                <div className="flex-1">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Região</label>
                  <select value={selRegiao} onChange={e => setSelRegiao(e.target.value)} className={inputClass}>
                    <option value="Todas">Todas</option><option value="Norte">Norte</option><option value="Nordeste">Nordeste</option>
                    <option value="Centro-Oeste">Centro-Oeste</option><option value="Sudeste">Sudeste</option><option value="Sul">Sul</option>
                  </select>
                </div>
                <div className="flex-1">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Estado</label>
                  <select value={selEstado} onChange={e => setSelEstado(e.target.value)} className={inputClass}>
                    <option value="Todos">Todos</option>
                    {Array.from(new Set(dadosBase.map(d => d.estado))).sort().map(uf => <option key={uf} value={uf}>{uf}</option>)}
                  </select>
                </div>
                <div className="flex-1">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Tamanho da Terra</label>
                  <select value={selCategoria} onChange={e => setSelCategoria(e.target.value)} className={inputClass}>
                    <option value="Todos">Todos</option>
                    {Array.from(new Set(dadosBase.map(d => d.categoria_tamanho))).sort().map(cat => <option key={cat} value={cat}>{cat}</option>)}
                  </select>
                </div>
                <div className="flex-1">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Buscar Município</label>
                  <input type="text" placeholder="Ex: Abadiânia..." value={buscaMunicipio} onChange={e => setBuscaMunicipio(e.target.value)} className={inputClass} />
                </div>
              </div>

              {loadingDados ? (
                <div className="text-center p-10 font-medium text-muted-foreground animate-pulse">Carregando base de dados do INCRA...</div>
              ) : (
                <>
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                    {[
                      { t: "Mediana (R$/ha)", v: formatBRL(kpis.mediana), i: BarChart3, c: "bg-white border-l-4 border-l-primary" },
                      { t: "Preço Médio (R$/ha)", v: formatBRL(kpis.media), i: TrendingUp, c: "bg-accent border" },
                      { t: "Volume Amostral", v: kpis.amostras.toLocaleString('pt-BR'), i: MapPin, c: "bg-white border" },
                      { t: "Dispersão Média", v: `${kpis.dispersao.toFixed(1)}%`, i: BarChart3, c: "bg-[#fffaeb] border border-[#fef0c7]" },
                    ].map((k, i) => (
                      <div key={i} className={`${k.c} p-6 rounded-xl shadow-sm border-border flex justify-between`}>
                        <div><p className="text-xs font-bold text-muted-foreground uppercase">{k.t}</p><p className="text-2xl font-black mt-2 text-foreground">{k.v}</p></div>
                        <k.i size={32} className="text-primary opacity-20" />
                      </div>
                    ))}
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 h-[500px]">
                    <div className="bg-white rounded-xl border border-border shadow-sm overflow-hidden flex flex-col relative z-0">
                      <div className="p-4 border-b border-border font-bold">Mapa Geográfico</div>
                      <div className="flex-1 relative z-0">
                        <MapContainer center={[-15.78, -47.92]} zoom={4} style={{ height: '100%', width: '100%', zIndex: 0 }}>
                          <TileLayer url="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png" />
                          {dadosFiltrados.slice(0, 1000).map((d, i) => (
                            <CircleMarker key={i} center={[d.lat, d.lon]} radius={6} pathOptions={{ fillColor: '#005826', color: '#005826', fillOpacity: 0.7 }}>
                              <LeafletTooltip><b>{d.municipio} - {d.estado}</b><br/>{formatBRL(d.mediana_geral)}</LeafletTooltip>
                            </CircleMarker>
                          ))}
                        </MapContainer>
                      </div>
                    </div>

                    <div className="bg-white rounded-xl border border-border shadow-sm flex flex-col lg:col-span-2 overflow-hidden">
                      <div className="p-4 border-b border-border font-bold">Dados Detalhados ({dadosFiltrados.length})</div>
                      <div className="flex-1 overflow-auto">
                        <table className="w-full text-sm text-left">
                          <thead className="bg-muted sticky top-0 text-xs uppercase font-bold text-muted-foreground shadow-sm">
                            <tr><th className="px-4 py-3">Município</th><th className="px-4 py-3">UF</th><th className="px-4 py-3">Categoria</th><th className="px-4 py-3 text-right">Amostras</th><th className="px-4 py-3 text-right">Mediana</th><th className="px-4 py-3 text-right">Dispersão</th></tr>
                          </thead>
                          <tbody className="divide-y divide-border">
                            {dadosFiltrados.slice(0, 100).map((r, i) => (
                              <tr key={i} className="hover:bg-muted/50">
                                <td className="px-4 py-3 font-medium">{r.municipio}</td><td className="px-4 py-3">{r.estado}</td>
                                <td className="px-4 py-3 text-muted-foreground">{r.categoria_tamanho}</td><td className="px-4 py-3 text-right font-medium">{r.total_anuncios_reais}</td>
                                <td className="px-4 py-3 text-right text-primary font-bold">{formatBRL(r.mediana_geral)}</td><td className="px-4 py-3 text-right">{r.coef_dispersao_pct ? `${r.coef_dispersao_pct}%` : '-'}</td>
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
        )}

        {/* ========================================================= */}
        {/* ABA 2: CENTRAL DE COMANDO (ROBÔS E IA)                    */}
        {/* ========================================================= */}
        {abaAtiva === "comando" && (
          <>
            <header className="bg-white border-b border-border px-8 py-6 flex flex-col shrink-0">
              <h1 className="text-2xl font-bold">Central de Comando</h1>
              <p className="text-muted-foreground text-sm">Controle operacional dos motores de raspagem e IA</p>
            </header>

            <div className="flex-1 overflow-auto p-8 space-y-6">
              
              <div className="bg-white p-6 rounded-xl border border-border shadow-sm flex flex-col md:flex-row gap-6 items-end">
                <div className="flex-1">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Tipo de Operação</label>
                  <select disabled={roboRodando} value={selOperacao} onChange={e => setSelOperacao(e.target.value)} className={inputClass}>
                    <option value="full">Completo (LFP + ECI)</option>
                    <option value="lfp">Apenas Mapear Links (LFP)</option>
                    <option value="eci">Apenas Inteligência Artificial (ECI)</option>
                    <option value="aci">Fallback Segurança (ACI)</option>
                    <option value="audit">Auditoria de Rota (Check)</option>
                    <option value="test">Teste de Conexão Banco</option>
                  </select>
                </div>
                
                <div className="w-48">
                  <label className="text-xs font-bold text-muted-foreground uppercase mb-1 block">Poder de Processamento</label>
                  <select disabled={roboRodando || !["full", "eci"].includes(selOperacao)} value={selWorkers} onChange={e => setSelWorkers(e.target.value)} className={inputClass}>
                    {[1,2,3,4,5].map(w => <option key={w} value={w}>{w} Worker{w>1?'s':''}</option>)}
                  </select>
                </div>

                <div className="w-48">
                  {!roboRodando ? (
                    <button onClick={handleIniciarRobo} className="w-full bg-primary hover:bg-primary/90 text-white font-bold py-2 px-4 rounded-md shadow flex justify-center items-center gap-2">
                      <Cpu size={18} /> Iniciar Motor
                    </button>
                  ) : (
                    <button onClick={handlePararRobo} className="w-full bg-red-600 hover:bg-red-700 text-white font-bold py-2 px-4 rounded-md shadow flex justify-center items-center gap-2">
                      <AlertTriangle size={18} /> Abortar Operação
                    </button>
                  )}
                </div>
              </div>

              {/* Console de Logs */}
              <div className="bg-[#1e1e1e] rounded-xl border border-border shadow-sm overflow-hidden flex flex-col h-[500px]">
                <div className="bg-[#2d2d2d] p-3 border-b border-[#404040] flex items-center gap-2 text-gray-300 font-mono text-sm">
                  <TerminalSquare size={16} /> Console do Orquestrador
                  {roboRodando && <span className="ml-auto flex h-3 w-3"><span className="animate-ping absolute inline-flex h-3 w-3 rounded-full bg-green-400 opacity-75"></span><span className="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span></span>}
                </div>
                <div className="flex-1 p-4 overflow-y-auto font-mono text-sm text-green-400 whitespace-pre-wrap">
                  {logsRobo.length === 0 ? (
                    <span className="text-gray-500">Sistema em Standby. Aguardando comandos...</span>
                  ) : (
                    logsRobo.map((log, i) => (
                      <div key={i} className="mb-1">{log}</div>
                    ))
                  )}
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}