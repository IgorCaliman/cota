import streamlit as st
import os
import time
import json
import requests
import zipfile
import io
import pandas as pd
import yfinance as yf
import xml.etree.ElementTree as ET
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from workalendar.america import Brazil

# ============================== DADOS DE CLASSIFICAÇÃO SETORIAL ==============================
# Versão final da lista de empresas e setores, com todas as modificações aplicadas.

dados_setoriais = [
    # Novo Grupo Simpar
    {"SETOR": "Grupo Simpar", "CODIGO": "MOVI3"},
    {"SETOR": "Grupo Simpar", "CODIGO": "VAMO3"},
    {"SETOR": "Grupo Simpar", "CODIGO": "JSLG3"},
    {"SETOR": "Grupo Simpar", "CODIGO": "SIMH3"},
    {"SETOR": "Grupo Simpar", "CODIGO": "AMOB3"},

    # Educação
    {"SETOR": "Serviços Educacionais", "CODIGO": "ANIM3"},
    {"SETOR": "Serviços Educacionais", "CODIGO": "COGN3"},
    {"SETOR": "Serviços Educacionais", "CODIGO": "VTRU3"},
    {"SETOR": "Serviços Educacionais", "CODIGO": "YDUQ3"},
    
    # Energia Elétrica
    {"SETOR": "Energia Elétrica", "CODIGO": "NEOE3"},
    {"SETOR": "Energia Elétrica", "CODIGO": "ALUP11"},
    {"SETOR": "Energia Elétrica", "CODIGO": "ELET3"},
    {"SETOR": "Energia Elétrica", "CODIGO": "ENGI11"},
    {"SETOR": "Energia Elétrica", "CODIGO": "CMIG3"},
    {"SETOR": "Energia Elétrica", "CODIGO": "CPLE6"},

    # Oil and Gas
    {"SETOR": "Exploração, Refino e Distribuição", "CODIGO": "BRAV3"},
    {"SETOR": "Exploração, Refino e Distribuição", "CODIGO": "CSAN3"},
    {"SETOR": "Exploração, Refino e Distribuição", "CODIGO": "PETR4"},
    {"SETOR": "Exploração, Refino e Distribuição", "CODIGO": "PRIO3"},
    {"SETOR": "Exploração, Refino e Distribuição", "CODIGO": "RECV3"},

    # Celulose
    {"SETOR": "Papel e Celulose", "CODIGO": "KLBN11"},
    {"SETOR": "Papel e Celulose", "CODIGO": "SUZB3"},

    # Real State (antigo Exploração de Imóveis e Incorporações)
    {"SETOR": "Real State", "CODIGO": "ALOS3"},
    {"SETOR": "Real State", "CODIGO": "EZTC3"},
    {"SETOR": "Real State", "CODIGO": "HBSA3"},
    {"SETOR": "Real State", "CODIGO": "LOGG3"},
    {"SETOR": "Real State", "CODIGO": "MELK3"},

    # Bancos
    {"SETOR": "Bancos", "CODIGO": "ITSA4"},
    {"SETOR": "Bancos", "CODIGO": "ABCB4"},
    {"SETOR": "Bancos", "CODIGO": "BBAS3"},
    {"SETOR": "Bancos", "CODIGO": "BBDC4"},
    {"SETOR": "Bancos", "CODIGO": "BPAC11"},
    {"SETOR": "Bancos", "CODIGO": "BRBI11"},
    {"SETOR": "Bancos", "CODIGO": "BRSR6"},
    {"SETOR": "Bancos", "CODIGO": "CASH3"},
    {"SETOR": "Bancos", "CODIGO": "ITUB4"},
    
    # Material Rodoviário
    {"SETOR": "Material Rodoviário", "CODIGO": "MYPK3"},
    {"SETOR": "Material Rodoviário", "CODIGO": "RAPT4"},
    {"SETOR": "Material Rodoviário", "CODIGO": "TUPY3"},

    # BDRs
    {"SETOR": "BDR – Setor internacional", "CODIGO": "AAPL34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "AMZO34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "BABA34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "BERK34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "BIEV39"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "BOAC34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "COLG34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "GOGL34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "GOGL35"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "JPMC34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "M2ST34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "M1TA34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "MCDC34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "NFLX34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "NVDC34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "PEPB34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "PFIZ34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "PGCO34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "S1PO34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "TSLA34"},
    {"SETOR": "BDR – Setor internacional", "CODIGO": "WALM34"},

    # Demais
]

df_setorial = pd.DataFrame(dados_setoriais)

# ============================== CONFIGURAÇÕES GLOBAIS ============================== #
TIPO_RELATORIO = 3
TEMPO_ESPERA = 30
PASTA_DESTINO = "download_XML"
CNPJ_MINAS_FIA = "FD11209172000196"
DATA_MARCA_DAGUA_STR = "02/01/2024"
DATA_MARCA_DAGUA_API = "2024-01-02"

FUNDOS = {
    CNPJ_MINAS_FIA: {
        "nome": "MINAS FIA",
        "cota_inicio": 1.9477472,
        "cota_ytd": 1.8726972,
        "marca_dagua": 3.0196718,
    },
    "FD60096402000163": {"nome": "MINAS DIVIDENDOS FIA"},
    "FD52204085000123": {"nome": "MINAS ONE FIA"},
    "FD48992682000192": {"nome": "ALFA HORIZON FIA"},
}
COLUNAS_EXIBIDAS = ["Ticker", "Quantidade de Ações", "Preço Ontem (R$)", "Preço Hoje (R$)", "% no Fundo",
                    "Variação Preço (%)", "Variação Ponderada (%)"]


# ============================== FUNÇÕES DE LOGIN ============================== #
def credenciais_inseridas():
    if "senha_login" not in st.secrets:
        st.error("A chave 'senha_login' não foi encontrada nos segredos do Streamlit.")
        return

    usuarios_validos = {
        "admin": st.secrets["senha_login"]
    }
    usuario_inserido = st.session_state.get("user_input", "").lower()
    senha_inserida = st.session_state.get("password_input", "")
    if usuario_inserido in usuarios_validos and usuarios_validos[usuario_inserido] == senha_inserida:
        st.session_state["authenticated"] = True
        st.session_state["username"] = usuario_inserido
    else:
        st.session_state["authenticated"] = False
        if not usuario_inserido and not senha_inserida:
            pass
        else:
            st.error("Usuário ou senha inválido.")


def autenticar_usuario():
    if "authenticated" not in st.session_state: st.session_state["authenticated"] = False
    if st.session_state["authenticated"]: return True
    st.text_input(label="Usuário", key="user_input")
    st.text_input(label="Senha", type="password", key="password_input")
    if st.button("Entrar"): credenciais_inseridas(); st.rerun()
    return False


# ============================== FUNÇÕES DE PROCESSAMENTO DE DADOS ============================== #
@st.cache_data(show_spinner="Obtendo carteiras do dia do BTG (só na 1ª vez)...", ttl=86400)
def obter_dados_base_do_dia(data_str: str):
    token = gerar_token()
    if not token: return {}
    ticket = gerar_ticket(token, data_str)
    mapeamento_xmls = baixar_xmls(token, ticket)

    dados_base = {}
    if mapeamento_xmls:
        for cnpj, xml_path in mapeamento_xmls.items():
            df_base, cota_ontem, qtd_cotas, pl = extrair_xml(xml_path)
            dados_base[cnpj] = {
                "df_base": df_base, "cota_ontem": cota_ontem,
                "qtd_cotas": qtd_cotas, "pl": pl
            }
    return dados_base


@st.cache_data(ttl=86400)
def get_cdi_acumulado(data_inicio: str, data_fim: str) -> float:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados?formato=json&dataInicial={data_inicio}&dataFinal={data_fim}"
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            dados_cdi = response.json()
            if not dados_cdi: return 0.0
            fator_acumulado = 1.0
            for dado in dados_cdi:
                fator_acumulado *= (1 + (float(dado['valor']) / 100))
            return fator_acumulado - 1
        except requests.RequestException as e:
            if attempt < 2:
                time.sleep(1)
            else:
                st.error(f"Erro ao buscar dados do CDI após 3 tentativas: {e}")
    return 0.0


@st.cache_data(ttl=86400)
def get_ibov_acumulado(data_inicio: str, data_fim: str) -> float:
    try:
        dados_ibov = yf.download('^BVSP', start=data_inicio, end=data_fim, progress=False, auto_adjust=True)
        if dados_ibov.empty or len(dados_ibov) < 2: return 0.0
        preco_inicio = float(dados_ibov['Close'].iloc[0])
        preco_fim = float(dados_ibov['Close'].iloc[-1])
        return (preco_fim / preco_inicio) - 1
    except Exception as e:
        st.error(f"Erro ao buscar dados do IBOV: {e}")
        return 0.0


def recalcular_metricas(df_base, cota_ontem, qtd_cotas, pl):
    df = df_base.copy()
    df["Preço Hoje (R$)"] = df["Ticker"].map(lambda t: yf.Ticker(f"{t}.SA").info.get("regularMarketPrice", None))
    df["Variação Preço (%)"] = (df["Preço Hoje (R$)"] / df["Preço Ontem (R$)"] - 1).fillna(0)
    df["Valor Hoje (R$)"] = df["Quantidade de Ações"] * df["Preço Hoje (R$)"]
    valor_hoje = df["Valor Hoje (R$)"].fillna(0).sum()
    df["% no Fundo"] = df["Valor Hoje (R$)"] / valor_hoje if valor_hoje != 0 else 0
    df["Variação Ponderada (%)"] = df["Variação Preço (%)"] * df["% no Fundo"]
    valor_ontem, comp_fixos = df["Valor Ontem (R$)"].sum(), pl - df["Valor Ontem (R$)"].sum()
    patrimonio = valor_hoje + comp_fixos
    cota_hoje = patrimonio / qtd_cotas if qtd_cotas != 0 else 0
    var_cota = cota_hoje / cota_ontem - 1 if cota_ontem != 0 else 0
    return {"df": df, "cota_hoje": cota_hoje, "var_cota": var_cota,
            "extras": {"valor_ontem": valor_ontem, "valor_hoje": valor_hoje, "comp_fixos": comp_fixos,
                       "patrimonio": patrimonio, "qtd_cotas": qtd_cotas}}


@st.cache_data(show_spinner="Buscando preços e calculando performance...", ttl=900)
def buscar_precos_empresas(tickers: list[str]):
    """
    Busca dados de D-1, D-0, volatilidade e a performance em vários períodos.
    """
    try:
        # Período de 4 anos para garantir dados para todos os cálculos
        periodo_longo = "4y"
        dados = yf.download(tickers, period=periodo_longo, progress=False, auto_adjust=True)

        if dados.empty:
            st.warning("Não foi possível obter dados históricos via yfinance.")
            return pd.DataFrame()

        precos_historicos = dados['Close']
        if precos_historicos.empty:
            return pd.DataFrame()

        # --- Cálculos de Performance ---
        hoje = precos_historicos.index[-1]
        datas_inicio = {
            "1M": hoje - relativedelta(months=1),
            "6M": hoje - relativedelta(months=6),
            "YTD": datetime(hoje.year, 1, 1),
            "1A": hoje - relativedelta(years=1),
            "3A": hoje - relativedelta(years=3)
        }

        # Preço final é sempre o mais recente
        preco_final = precos_historicos.iloc[-1]

        variacoes = {}
        for nome, data_inicio in datas_inicio.items():
            # Encontra o índice da primeira data de pregão >= à data de início calculada
            idx_inicio = precos_historicos.index.searchsorted(data_inicio)

            # Garante que o índice não está fora dos limites
            if idx_inicio < len(precos_historicos):
                preco_inicial = precos_historicos.iloc[idx_inicio]
                # Cálculo vetorizado para todas as ações de uma vez
                variacoes[nome] = (preco_final / preco_inicial) - 1
            else:
                # Se não houver dados para o período, preenche com zero ou NaN
                variacoes[nome] = pd.Series(0, index=precos_historicos.columns)

        df_variacoes = pd.DataFrame(variacoes)

        # --- Cálculo da Volatilidade ---
        retornos_diarios = precos_historicos.pct_change()
        volatilidade_60d = retornos_diarios.iloc[-60:].std()

        # --- Extração de Preços (Ontem e Hoje) ---
        preco_ontem = precos_historicos.iloc[-2]
        preco_hoje = precos_historicos.iloc[-1]

        # --- Montagem do DataFrame Final ---
        df_resultado = pd.DataFrame({
            'Preço Ontem (R$)': preco_ontem,
            'Preço Hoje (R$)': preco_hoje,
            'Variação (%)': (preco_hoje / preco_ontem) - 1,
            'Volatilidade (60d)': volatilidade_60d
        })

        # Junta os dataframes de resultado e de variações pelo índice (ticker)
        df_resultado = df_resultado.join(df_variacoes)
        df_resultado.reset_index(inplace=True)
        df_resultado.rename(columns={'index': 'Ticker'}, inplace=True)

        return df_resultado

    except Exception as e:
        st.error(f"Ocorreu um erro ao buscar os preços no yfinance: {e}")
        return pd.DataFrame()


# ============================== FUNÇÕES AUXILIARES ============================== #
def ultimo_dia_util(delay: int = 1) -> str:
    cal, d = Brazil(), pd.Timestamp.now(tz="America/Sao_Paulo") - timedelta(days=delay)
    while not cal.is_working_day(d.date()): d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


@st.cache_data(ttl=3600)
def gerar_token():
    if "senha_af" not in st.secrets:
        st.error("A chave 'senha_af' não foi encontrada nos segredos do Streamlit.")
        return None
    try:
        resp = requests.post("https://funds.btgpactual.com/connect/token",
                             headers={"Content-Type": "application/x-www-form-urlencoded"},
                             data=st.secrets["senha_af"])
        resp.raise_for_status()
        return resp.json()["access_token"]
    except requests.RequestException as e:
        st.error(f"Falha ao obter token do BTG: {e}")
        return None


def gerar_ticket(token, data):
    payload = json.dumps({"contract": {"startDate": data, "endDate": data, "typeReport": f"{TIPO_RELATORIO}"}})
    resp = requests.post("https://funds.btgpactual.com/reports/Portfolio",
                         headers={"X-SecureConnect-Token": f"Bearer {token}", "Content-Type": "application/json"},
                         data=payload)
    return resp.json()["ticket"]


def baixar_xmls(token, ticket) -> dict[str, str]:
    os.makedirs(PASTA_DESTINO, exist_ok=True)
    url = f"https://funds.btgpactual.com/reports/Ticket?ticketId={ticket}"
    time.sleep(TEMPO_ESPERA)
    resp = requests.get(url, headers={"X-SecureConnect-Token": f"Bearer {token}"})
    mapeamento = {}
    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(PASTA_DESTINO)
        for nome in os.listdir(PASTA_DESTINO):
            caminho, cnpj_arquivo = os.path.join(PASTA_DESTINO, nome), nome.split("_")[0]
            if cnpj_arquivo in FUNDOS:
                mapeamento[cnpj_arquivo] = caminho
            else:
                os.remove(caminho)
    except (zipfile.BadZipFile, KeyError):
        st.error("❌ ZIP inválido ou indisponível no BTG. Tente novamente mais tarde.")
    return mapeamento


def extrair_xml(path):
    root = ET.parse(path).getroot()
    head = root.find(".//header")
    cota_ontem, qtd_cotas, pl = float(head.findtext("valorcota")), float(head.findtext("quantidade")), float(
        head.findtext("patliq"))
    linhas = [{"Ticker": ac.findtext("codativo").strip(), "Quantidade de Ações": float(ac.findtext("qtdisponivel")),
               "Preço Ontem (R$)": float(ac.findtext("puposicao")),
               "Valor Ontem (R$)": float(ac.findtext("qtdisponivel")) * float(ac.findtext("puposicao"))} for ac in
              root.findall(".//acoes")]
    return pd.DataFrame(linhas), cota_ontem, qtd_cotas, pl


def css_var(v):
    if isinstance(v, (float, int)):
        if v > 0: return "color: green;"
        if v < 0: return "color: red;"
    return ""


# ============================== INTERFACE STREAMLIT ============================== #
st.set_page_config("Carteiras RV AF INVEST", layout="wide")

if autenticar_usuario():
    data_carteira_str = ultimo_dia_util()
    data_formatada = datetime.strptime(data_carteira_str, '%Y-%m-%d').strftime('%d/%m/%Y')
    st.title(f"AF INVEST | Análise de Carteiras e Ações")
    st.caption(f"Posição dos fundos referente ao dia: {data_formatada}")
    st.write(f"Usuário: **{st.session_state.get('username', '').capitalize()}**")

    tab_fundos, tab_empresas = st.tabs(["📊 Análise de Fundos", "📈 Acompanhamento de Empresas"])

    # ============================== ABA DE ANÁLISE DE FUNDOS ============================== #
    with tab_fundos:
        st.session_state.setdefault('dados_calculados_cache', {})
        st.session_state.setdefault('global_last_update_time', None)

        dados_base_do_dia = obter_dados_base_do_dia(ultimo_dia_util())

        if not dados_base_do_dia:
            st.error(
                "Não foi possível obter os dados da carteira do BTG. Verifique os CNPJs ou a disponibilidade no portal.")

            if st.button("🔄 Tentar buscar dados do BTG novamente"):
                st.cache_data.clear()
                st.rerun()
        else:
            ordem_especifica = [
                CNPJ_MINAS_FIA,
                "FD60096402000163",
                "FD52204085000123",
                "FD48992682000192"
            ]
            opcoes_ordenadas = [cnpj for cnpj in ordem_especifica if cnpj in dados_base_do_dia]
            nomes_fundos = {cnpj: FUNDOS[cnpj]["nome"] for cnpj in opcoes_ordenadas}

            summary_container = st.container()

            cnpj_selecionado = st.selectbox("Selecione o fundo para visualizar:", options=opcoes_ordenadas,
                                            format_func=lambda c: nomes_fundos.get(c, "Nome não encontrado"),
                                            key="fundo_selectbox")

            col_header, col_actions = st.columns([3, 2])
            with col_header:
                st.subheader(f"📊 Detalhes do Fundo — {FUNDOS[cnpj_selecionado]['nome']}")
            with col_actions:
                btn1, btn2 = st.columns(2)

                with btn1:
                    atualizar = st.button("🔄 Atualizar Preços dos Fundos")
                    if st.session_state.global_last_update_time:
                        st.caption(f"Preços atualizados às {st.session_state.global_last_update_time:%H:%M:%S}")

                with btn2:
                    if st.button("📥 Puxar Carteira BTG"):
                        with st.spinner("Limpando cache e buscando novamente os dados do BTG..."):
                            st.cache_data.clear()
                        st.rerun()
                    st.caption("Puxe quando o preço D-1 parecer estranho.")

            is_cache_incomplete = len(st.session_state.dados_calculados_cache) != len(dados_base_do_dia)
            if atualizar or is_cache_incomplete:
                with st.spinner("Atualizando os preços de todos os fundos..."):
                    for cnpj, dados_base_fundo in dados_base_do_dia.items():
                        resultados = recalcular_metricas(dados_base_fundo["df_base"],
                                                          dados_base_fundo["cota_ontem"],
                                                          dados_base_fundo["qtd_cotas"], dados_base_fundo["pl"])
                        st.session_state.dados_calculados_cache[cnpj] = resultados

                st.session_state.global_last_update_time = datetime.now(tz=ZoneInfo("America/Sao_Paulo"))
                st.rerun()

            if cnpj_selecionado in st.session_state.dados_calculados_cache:
                with summary_container:
                    st.subheader("Resumo das Variações dos Fundos")
                    summary_data = []
                    for cnpj in ordem_especifica:
                        if cnpj in st.session_state.dados_calculados_cache:
                            fund_name = FUNDOS[cnpj]["nome"]
                            variation = st.session_state.dados_calculados_cache[cnpj]['var_cota']
                            summary_data.append({"Fundo": fund_name, "Variação da Cota": variation})

                    if summary_data:
                        summary_df = pd.DataFrame(summary_data)

                        def style_variation(v):
                            color = 'green' if v > 0 else 'red' if v < 0 else 'darkgray'
                            return f'color: {color}'

                        st.dataframe(
                            summary_df.style.map(style_variation, subset=['Variação da Cota']).format(
                                {"Variação da Cota": "{:.4%}"}),
                            use_container_width=True,
                            hide_index=True
                        )
                    st.divider()

                dados_calculados, cota_ontem_base = st.session_state.dados_calculados_cache[cnpj_selecionado], \
                    dados_base_do_dia[cnpj_selecionado]['cota_ontem']
                df_final = dados_calculados["df"]

                fmt = {"Quantidade de Ações": "{:,.0f}", "Preço Ontem (R$)": "R$ {:.2f}",
                       "Preço Hoje (R$)": "R$ {:.2f}",
                       "% no Fundo": "{:.2%}", "Variação Preço (%)": "{:.2%}", "Variação Ponderada (%)": "{:.2%}"}
                st.dataframe(
                    df_final[COLUNAS_EXIBIDAS].sort_values("% no Fundo", ascending=False).style.format(fmt).map(css_var,
                                                                                                               subset=[
                                                                                                                   "Variação Preço (%)",
                                                                                                                   "Variação Ponderada (%)"]),
                    use_container_width=True, hide_index=True)

                c1, c2, c3 = st.columns(3)
                c1.metric("Cota de Ontem", f"R$ {cota_ontem_base:.6f}")
                c2.metric("Cota Estimada Hoje", f"R$ {dados_calculados['cota_hoje']:.6f}")
                c3.metric("Variação da Cota", f"{dados_calculados['var_cota']:.4%}")

                if cnpj_selecionado == CNPJ_MINAS_FIA:
                    st.divider()
                    cota_hoje = dados_calculados['cota_hoje']
                    ref_minas_fia = FUNDOS[CNPJ_MINAS_FIA]
                    rent_ytd = (cota_hoje / ref_minas_fia['cota_ytd'] - 1) if ref_minas_fia['cota_ytd'] > 0 else 0
                    rent_inicio = (cota_hoje / ref_minas_fia['cota_inicio'] - 1) if ref_minas_fia[
                                                                                       'cota_inicio'] > 0 else 0
                    hoje_str, hoje_dt = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime(
                        '%d/%m/%Y'), datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d')
                    cdi_acumulado = get_cdi_acumulado(data_inicio="15/10/2020", data_fim=hoje_str)
                    ibov_acumulado_inicio = get_ibov_acumulado(data_inicio="2020-10-15", data_fim=hoje_dt)
                    percentual_cdi = rent_inicio - cdi_acumulado
                    marca_dagua = ref_minas_fia['marca_dagua']
                    falta_marca_dagua = (marca_dagua / cota_hoje - 1) if cota_hoje > 0 else 0
                    ibov_desde_marca_dagua = get_ibov_acumulado(data_inicio=DATA_MARCA_DAGUA_API, data_fim=hoje_dt)
                    falta_total = falta_marca_dagua + ibov_desde_marca_dagua

                    st.subheader("Análise de Rentabilidade — MINAS FIA")
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Rent. YTD", f"{rent_ytd:.2%}")
                    m2.metric("Rent. Início (15/10/20)", f"{rent_inicio:.2%}")
                    m3.metric("CDI no período (15/10/20)", f"{cdi_acumulado:.2%}")
                    m4.metric("IBOV no período (15/10/20)", f"{ibov_acumulado_inicio:.2%}")

                    md_label = f"M. d'Água ({DATA_MARCA_DAGUA_STR})"
                    col_md_1, col_md_2, col_md_3 = st.columns(3)
                    col_md_1.metric(f"Falta p/ {md_label}", f"{falta_marca_dagua:.2%}")
                    col_md_2.metric(f"IBOV desde {md_label}", f"{ibov_desde_marca_dagua:.2%}")
                    col_md_3.metric(f"Falta p/ {md_label} + IBOV", f"{falta_total:.2%}")

                    texto_relativo_cdi = "acima do CDI" if percentual_cdi >= 0 else "abaixo do CDI"
                    valor_display_cdi = f"{abs(percentual_cdi):.2%} {texto_relativo_cdi}"

                    st.metric("Performance vs CDI (desde 15/10/2020)", valor_display_cdi,
                              delta=f"{percentual_cdi:.2%}", delta_color="off")

                with st.expander("🔍 Parâmetros do Cálculo"):
                    ex = dados_calculados["extras"]
                    st.write(f"📌 Valor das ações ontem: R$ {ex['valor_ontem']:,.2f}")
                    st.write(f"📌 Valor das ações hoje:  R$ {ex['valor_hoje']:,.2f}")
                    st.write(f"📎 Componentes fixos:     R$ {ex['comp_fixos']:,.2f}")
                    st.write(f"💼 Patrimônio estimado:  R$ {ex['patrimonio']:,.2f}")
                    st.write(f"🧮 Quantidade de cotas:  {ex['qtd_cotas']:,.2f}")

    # ============================== ABA DE ACOMPANHAMENTO DE EMPRESAS ============================== #
    with tab_empresas:
        st.subheader("Análise de Performance por Setor")
        st.markdown("---")
    
        # --- LÓGICA DE ORDENAÇÃO DOS SETORES ---
        # Ordem de exibição definida pelo usuário
        ordem_desejada = [
            "Grupo Simpar",
            "Serviços Educacionais",
            "Papel e Celulose",
            "Energia Elétrica",
            "Real State",
            "Material Rodoviário"
        ]
        setor_bdr = "BDR – Setor internacional"
        
        # Pega todos os setores únicos da nossa lista de dados
        todos_setores = df_setorial['SETOR'].unique().tolist()
        
        # Lógica para encontrar setores "esquecidos" e garantir a ordem final
        setores_nao_ordenados = [s for s in todos_setores if s not in ordem_desejada and s != setor_bdr]
        setores_nao_ordenados.sort() # Ordena os esquecidos alfabeticamente
        
        # Monta a lista final na ordem correta
        setores_ordenados = ordem_desejada + setores_nao_ordenados
        if setor_bdr in todos_setores:
            setores_ordenados.append(setor_bdr)
    
        # Loop para criar uma tabela para cada setor, na ordem definida
        for setor in setores_ordenados:
            st.subheader(f"Setor: {setor}")
    
            # Pega os tickers apenas para o setor atual
            df_setor_atual = df_setorial[df_setorial['SETOR'] == setor]
            tickers_do_setor = df_setor_atual['CODIGO'].tolist()
            tickers_para_api = [ticker + '.SA' for ticker in tickers_do_setor]
    
            # Busca os dados de performance para os tickers do setor
            df_performance = buscar_precos_empresas(tickers_para_api)
    
            if not df_performance.empty:
                df_display = df_performance.copy()
                df_display['Ticker'] = df_display['Ticker'].str.replace(".SA", "", regex=False)
                df_display.rename(columns={
                    'Variação (%)': 'Var. Dia', 'Volatilidade (60d)': 'Vol (60d)',
                    '1M': 'Var. 1M', '6M': 'Var. 6M', '1A': 'Var. 1A', '3A': 'Var. 3A'
                }, inplace=True)
    
                formatos = {
                    "Preço Hoje (R$)": "R$ {:.2f}", "Var. Dia": "{:.2%}", "Vol (60d)": "{:.2%}",
                    "Var. 1M": "{:.2%}", "Var. 6M": "{:.2%}", "YTD": "{:.2%}", "Var. 1A": "{:.2%}", "Var. 3A": "{:.2%}"
                }
                colunas_para_remover = ['Preço Ontem (R$)']
                colunas_para_colorir = ['Var. Dia', 'Var. 1M', 'Var. 6M', 'YTD', 'Var. 1A', 'Var. 3A']
    
                for col in colunas_para_remover:
                    if col in df_display.columns:
                        del df_display[col]
    
                def estilo_variacao_empresa(v):
                    if isinstance(v, (int, float)):
                        cor = 'green' if v > 0 else 'red' if v < 0 else 'darkgray'
                        return f'color: {cor}'
                    return ''
    
                styler = df_display.style
                for col in colunas_para_colorir:
                    if col in df_display.columns:
                        styler = styler.applymap(estilo_variacao_empresa, subset=[col])
                styler = styler.format(formatos)
    
                st.dataframe(styler, use_container_width=True, hide_index=True)
    
            st.markdown("---")  # Divisor entre os setores
