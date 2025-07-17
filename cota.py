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
from zoneinfo import ZoneInfo
from workalendar.america import Brazil

# ============================== CONFIGURAÇÕES ============================== #
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

## NOVA ADIÇÃO: Lista de empresas para acompanhar na nova aba ##
EMPRESAS_ACOMPANHADAS = [
    'PETR4.SA', 'VALE3.SA', 'ITUB4.SA', 'BBDC4.SA', 
    'ABEV3.SA', 'WEGE3.SA', 'MGLU3.SA', 'LREN3.SA'
]


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


## NOVA ADIÇÃO: Função para buscar dados das empresas acompanhadas ##
# ==============================  COPIE E SUBSTITUA ESTA FUNÇÃO ==============================

@st.cache_data(show_spinner="Buscando preços das empresas...", ttl=900) # Cache de 15 minutos (900s)
def buscar_precos_empresas(tickers: list[str]):
    """
    Busca os dados de fechamento de D-1 e o preço atual para uma lista de tickers
    de forma robusta, garantindo o alinhamento dos dados.
    """
    try:
        # Puxa os dados dos últimos 2 dias. auto_adjust=True ajusta para splits/dividendos.
        dados = yf.download(tickers, period="2d", progress=False, auto_adjust=True)
        
        if dados.empty:
            st.warning("Não foi possível obter os dados de preços das empresas via yfinance.")
            return pd.DataFrame()

        # Isolar apenas os preços de fechamento ('Close'). 
        # O resultado é um DataFrame com datas no índice e tickers nas colunas.
        precos_df = dados['Close']

        # Se tivermos menos de 2 dias de dados (ex: uma segunda-feira de manhã),
        # tratamos para não dar erro.
        if len(precos_df) < 2:
            st.warning("Dados de apenas um dia disponíveis. Usando o mesmo valor para 'Ontem' e 'Hoje'.")
            preco_ontem = precos_df.iloc[0]
            preco_hoje = precos_df.iloc[0]
        else:
            preco_ontem = precos_df.iloc[-2] # Preços de D-2 (fechamento de ontem)
            preco_hoje = precos_df.iloc[-1]  # Preços de D-1 (fechamento mais recente)

        # Criar o DataFrame final a partir das séries de preços.
        # O Pandas alinha automaticamente os dados pelo índice (que são os tickers).
        df_resultado = pd.DataFrame({
            'Preço Ontem (R$)': preco_ontem,
            'Preço Hoje (R$)': preco_hoje
        })

        # Remove qualquer linha que tenha valores nulos (caso um ticker tenha falhado)
        df_resultado.dropna(inplace=True)

        # Calcula a variação
        df_resultado['Variação (%)'] = (df_resultado['Preço Hoje (R$)'] / df_resultado['Preço Ontem (R$)']) - 1

        # Transforma o índice (os tickers) em uma coluna e renomeia
        df_resultado.reset_index(inplace=True)
        df_resultado.rename(columns={'index': 'Ticker'}, inplace=True)
        
        # Reordena as colunas para a exibição final
        return df_resultado[['Ticker', 'Preço Ontem (R$)', 'Preço Hoje (R$)', 'Variação (%)']]

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
                             data= st.secrets["senha_af"])
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

    ## ALTERAÇÃO: Introdução do st.tabs para criar a navegação ##
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
                CNPJ_MINAS_FIA,      # MINAS FIA
                "FD60096402000163",  # MINAS DIVIDENDOS FIA
                "FD52204085000123",  # MINAS ONE FIA
                "FD48992682000192"   # ALFA HORIZON FIA
            ]
            opcoes_ordenadas = [cnpj for cnpj in ordem_especifica if cnpj in dados_base_do_dia]
            nomes_fundos = {cnpj: FUNDOS[cnpj]["nome"] for cnpj in opcoes_ordenadas}
            
            summary_container = st.container()
            
            cnpj_selecionado = st.selectbox("Selecione o fundo para visualizar:", options=opcoes_ordenadas,
                                              format_func=lambda c: nomes_fundos.get(c, "Nome não encontrado"), key="fundo_selectbox")

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
                        resultados = recalcular_metricas(dados_base_fundo["df_base"], dados_base_fundo["cota_ontem"],
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
                            summary_df.style.map(style_variation, subset=['Variação da Cota']).format({"Variação da Cota": "{:.4%}"}),
                            use_container_width=True,
                            hide_index=True
                        )
                    st.divider()

                dados_calculados, cota_ontem_base = st.session_state.dados_calculados_cache[cnpj_selecionado], \
                dados_base_do_dia[cnpj_selecionado]['cota_ontem']
                df_final = dados_calculados["df"]

                fmt = {"Quantidade de Ações": "{:,.0f}", "Preço Ontem (R$)": "R$ {:.2f}", "Preço Hoje (R$)": "R$ {:.2f}",
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
                    rent_inicio = (cota_hoje / ref_minas_fia['cota_inicio'] - 1) if ref_minas_fia['cota_inicio'] > 0 else 0
                    hoje_str, hoje_dt = datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime('%d/%m/%Y'), datetime.now(tz=ZoneInfo("America/Sao_Paulo")).strftime('%Y-%m-%d')
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
                    
                    st.metric("Performance vs CDI (desde 15/10/2020)", valor_display_cdi, delta=f"{percentual_cdi:.2%}", delta_color="off")

                with st.expander("🔍 Parâmetros do Cálculo"):
                    ex = dados_calculados["extras"]
                    st.write(f"📌 Valor das ações ontem: R$ {ex['valor_ontem']:,.2f}")
                    st.write(f"📌 Valor das ações hoje:  R$ {ex['valor_hoje']:,.2f}")
                    st.write(f"📎 Componentes fixos:     R$ {ex['comp_fixos']:,.2f}")
                    st.write(f"💼 Patrimônio estimado:  R$ {ex['patrimonio']:,.2f}")
                    st.write(f"🧮 Quantidade de cotas:  {ex['qtd_cotas']:,.2f}")

    # ============================== NOVA ADIÇÃO: ABA DE ACOMPANHAMENTO DE EMPRESAS ============================== #
    with tab_empresas:
        st.subheader("Acompanhamento da Variação de Empresas")

        # Botão de atualização específico para esta aba
        if st.button("🔄 Atualizar Preços das Empresas"):
            # Limpa o cache APENAS da função de buscar preços para forçar a atualização
            buscar_precos_empresas.clear()
        
        # Chama a função para obter os dados de preço
        df_empresas = buscar_precos_empresas(EMPRESAS_ACOMPANHADAS)

        if not df_empresas.empty:
            st.caption("Os preços 'Hoje' são atualizados a cada 15 minutos (ou ao clicar no botão de atualizar). O preço de 'Ontem' é o valor de fechamento do último pregão.")

            # Formatação e Estilo
            formatos_empresas = {
                "Preço Ontem (R$)": "R$ {:.2f}",
                "Preço Hoje (R$)": "R$ {:.2f}",
                "Variação (%)": "{:.2%}"
            }

            def estilo_variacao_empresa(v):
                if isinstance(v, (int, float)):
                    cor = 'green' if v > 0 else 'red' if v < 0 else 'darkgray'
                    return f'color: {cor}'
                return ''

            st.dataframe(
                df_empresas.style.applymap(
                    estilo_variacao_empresa, subset=['Variação (%)']
                ).format(formatos_empresas),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Aguardando dados das empresas. Clique no botão de atualização se necessário.")
