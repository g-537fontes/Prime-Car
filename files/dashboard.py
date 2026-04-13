import sqlite3
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="AutoPrime - Dashboard de Vendas",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded",
)

DW_PATH = "dw_autoprime.db"

if not os.path.exists(DW_PATH):
    st.error(
        f"Arquivo '{DW_PATH}' nao encontrado. "
        "Execute o pipeline ETL primeiro: python etl.py"
    )
    st.stop()


@st.cache_data(ttl=600)
def rodar_query(sql):
    with sqlite3.connect(DW_PATH) as conn:
        return pd.read_sql_query(sql, conn)


def formatar_usd(valor):
    return f"$ {valor:,.2f}"


def formatar_int(valor):
    return f"{int(valor):,}"


# ---------------------------------------------------------------
# SIDEBAR - FILTROS
# ---------------------------------------------------------------
st.sidebar.title("Filtros")

anos_disp   = rodar_query("SELECT DISTINCT ano FROM Dim_Tempo_Venda ORDER BY ano")["ano"].tolist()
meses_disp  = rodar_query("SELECT DISTINCT numero_mes, nome_mes FROM Dim_Tempo_Venda ORDER BY numero_mes")
trims_disp  = rodar_query("SELECT DISTINCT trimestre FROM Dim_Tempo_Venda ORDER BY trimestre")["trimestre"].tolist()
sems_disp   = rodar_query("SELECT DISTINCT semestre FROM Dim_Tempo_Venda ORDER BY semestre")["semestre"].tolist()
regioes_disp = rodar_query("SELECT DISTINCT regiao_loja FROM Dim_Loja_Venda ORDER BY regiao_loja")["regiao_loja"].tolist()

sel_anos    = st.sidebar.multiselect("Ano",       anos_disp,                      default=anos_disp)
sel_meses   = st.sidebar.multiselect("Mes",       meses_disp["nome_mes"].tolist(), default=meses_disp["nome_mes"].tolist())
sel_trims   = st.sidebar.multiselect("Trimestre", trims_disp,                     default=trims_disp)
sel_sems    = st.sidebar.multiselect("Semestre",  sems_disp,                      default=sems_disp)
sel_regioes = st.sidebar.multiselect("Regiao",    regioes_disp,                   default=regioes_disp)

st.sidebar.markdown("---")
st.sidebar.caption("AutoPrime | Laboratorio de Banco de Dados | Prof. Anderson Barroso")


def lista_sql(lst, aspas=False):
    if not lst:
        return "NULL"
    if aspas:
        return ", ".join(f"'{v}'" for v in lst)
    return ", ".join(str(v) for v in lst)


def filtro_where():
    return f"""
        t.ano          IN ({lista_sql(sel_anos)})
        AND t.nome_mes IN ({lista_sql(sel_meses, aspas=True)})
        AND t.trimestre IN ({lista_sql(sel_trims)})
        AND t.semestre  IN ({lista_sql(sel_sems)})
        AND l.regiao_loja IN ({lista_sql(sel_regioes, aspas=True)})
    """


# ---------------------------------------------------------------
# TITULO
# ---------------------------------------------------------------
st.title("AutoPrime - Dashboard de Vendas de Veiculos")
st.markdown(
    "Painel de analise de vendas da AutoPrime. "
    "Use os filtros na barra lateral para segmentar por periodo e regiao."
)
st.divider()


# ---------------------------------------------------------------
# CARDS - METRICAS GERAIS
# ---------------------------------------------------------------
sql_cards = f"""
    SELECT
        COALESCE(SUM(f.quantidade_vendida), 0) AS total_vendas,
        COALESCE(SUM(f.preco_venda), 0)        AS receita_total,
        COALESCE(AVG(f.preco_venda), 0)        AS ticket_medio,
        COUNT(DISTINCT v.marca)                AS total_marcas,
        COUNT(DISTINCT v.modelo)               AS total_modelos,
        COUNT(DISTINCT l.sk_loja)              AS total_lojas
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
"""
dados_cards = rodar_query(sql_cards).iloc[0]

# Marca e modelo que mais vendem
sql_top_marca = f"""
    SELECT v.marca, COUNT(*) AS total
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
    GROUP BY v.marca ORDER BY total DESC LIMIT 1
"""
sql_top_modelo = f"""
    SELECT v.modelo, COUNT(*) AS total
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
    GROUP BY v.modelo ORDER BY total DESC LIMIT 1
"""
df_top_marca  = rodar_query(sql_top_marca)
df_top_modelo = rodar_query(sql_top_modelo)
top_marca  = df_top_marca["marca"].iloc[0]  if not df_top_marca.empty  else "N/A"
top_modelo = df_top_modelo["modelo"].iloc[0] if not df_top_modelo.empty else "N/A"

c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
c1.metric("Total Vendas",   formatar_int(dados_cards["total_vendas"]))
c2.metric("Receita Total",  formatar_usd(dados_cards["receita_total"]))
c3.metric("Ticket Medio",   formatar_usd(dados_cards["ticket_medio"]))
c4.metric("Marcas",         formatar_int(dados_cards["total_marcas"]))
c5.metric("Modelos",        formatar_int(dados_cards["total_modelos"]))
c6.metric("Revendedores",   formatar_int(dados_cards["total_lojas"]))
c7.metric("Marca Top",      top_marca)
c8.metric("Modelo Top",     top_modelo)

st.divider()


# ---------------------------------------------------------------
# GRAFICO 1 - Evolucao do Preco Medio ao Longo do Tempo
# ---------------------------------------------------------------
st.subheader("Evolucao do Preco Medio de Venda por Periodo")

col_grafico, col_opcao = st.columns([3, 1])
with col_opcao:
    agrupamento = st.selectbox("Agrupar por", ["Mes/Ano", "Trimestre", "Semestre", "Ano"], key="g1")

if agrupamento == "Mes/Ano":
    sql1 = f"""
        SELECT t.ano, t.numero_mes, t.nome_mes,
               ROUND(AVG(f.preco_venda), 2)   AS preco_medio_venda,
               ROUND(AVG(f.preco_mercado), 2) AS preco_medio_mercado,
               COUNT(*) AS total
        FROM Fato_Vendas_Carros f
        JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
        JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
        JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
        WHERE {filtro_where()}
        GROUP BY t.ano, t.numero_mes, t.nome_mes
        ORDER BY t.ano, t.numero_mes
    """
    df1 = rodar_query(sql1)
    if not df1.empty:
        df1["periodo"] = df1["ano"].astype(str) + "-" + df1["numero_mes"].astype(str).str.zfill(2)
        fig1 = go.Figure()
        fig1.add_trace(go.Scatter(x=df1["periodo"], y=df1["preco_medio_venda"],
                                  mode="lines+markers", name="Preco de Venda",
                                  line=dict(color="#1f77b4", width=2)))
        fig1.add_trace(go.Scatter(x=df1["periodo"], y=df1["preco_medio_mercado"],
                                  mode="lines+markers", name="Preco de Mercado (MMR)",
                                  line=dict(color="#ff7f0e", width=2, dash="dash")))
        fig1.update_layout(
            xaxis_title="Periodo", yaxis_title="Preco Medio (USD)",
            xaxis_tickangle=-45, legend=dict(orientation="h"),
            hovermode="x unified"
        )
        with col_grafico:
            st.plotly_chart(fig1, use_container_width=True)
        st.caption(
            "Interpretacao: A linha azul mostra o preco medio praticado nas vendas e a linha "
            "laranja tracejada indica o preco de mercado (MMR). Quando a azul fica abaixo da "
            "laranja, os veiculos foram vendidos com desconto em relacao ao mercado."
        )
else:
    campo_map = {
        "Trimestre": ("trimestre", "Trimestre"),
        "Semestre":  ("semestre",  "Semestre"),
        "Ano":       ("ano",       "Ano"),
    }
    campo, label = campo_map[agrupamento]
    sql1b = f"""
        SELECT t.ano, t.{campo} AS periodo_val,
               ROUND(AVG(f.preco_venda), 2)   AS preco_medio_venda,
               ROUND(AVG(f.preco_mercado), 2) AS preco_medio_mercado
        FROM Fato_Vendas_Carros f
        JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
        JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
        JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
        WHERE {filtro_where()}
        GROUP BY t.ano, t.{campo}
        ORDER BY t.ano, t.{campo}
    """
    df1b = rodar_query(sql1b)
    if not df1b.empty:
        df1b["eixo"] = df1b["ano"].astype(str) + " " + label + " " + df1b["periodo_val"].astype(str)
        fig1b = go.Figure()
        fig1b.add_trace(go.Bar(x=df1b["eixo"], y=df1b["preco_medio_venda"],
                               name="Preco Venda", marker_color="#1f77b4"))
        fig1b.add_trace(go.Bar(x=df1b["eixo"], y=df1b["preco_medio_mercado"],
                               name="Preco Mercado", marker_color="#ff7f0e"))
        fig1b.update_layout(barmode="group", xaxis_tickangle=-45,
                            xaxis_title=label, yaxis_title="Preco Medio (USD)")
        with col_grafico:
            st.plotly_chart(fig1b, use_container_width=True)

st.divider()


# ---------------------------------------------------------------
# GRAFICO 2 - Distribuicao de Veiculos por Faixa de Preco
# ---------------------------------------------------------------
st.subheader("Distribuicao de Veiculos por Faixa de Preco de Venda")

sql2 = f"""
    SELECT f.preco_venda
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
"""
df2 = rodar_query(sql2)
if not df2.empty:
    fig2 = px.histogram(
        df2, x="preco_venda", nbins=60,
        labels={"preco_venda": "Preco de Venda (USD)", "count": "Quantidade"},
        color_discrete_sequence=["#2ca02c"], opacity=0.75,
    )
    fig2.update_layout(bargap=0.05, xaxis_title="Preco de Venda (USD)", yaxis_title="Quantidade")
    st.plotly_chart(fig2, use_container_width=True)
    st.caption(
        "Interpretacao: O histograma mostra a concentracao de vendas por faixa de preco. "
        "O pico indica a faixa de maior volume comercializado. "
        "A cauda longa a direita representa veiculos de alto valor."
    )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 3 e 4 - Categorias e Top 10 Marcas
# ---------------------------------------------------------------
col3, col4 = st.columns(2)

with col3:
    st.subheader("Ranking das Categorias Mais Ofertadas")
    sql3 = f"""
        SELECT v.categoria,
               SUM(f.quantidade_vendida)    AS total,
               ROUND(AVG(f.preco_venda), 2) AS preco_medio
        FROM Fato_Vendas_Carros f
        JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
        JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
        JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
        WHERE {filtro_where()}
        GROUP BY v.categoria
        ORDER BY total DESC
    """
    df3 = rodar_query(sql3)
    if not df3.empty:
        fig3 = px.pie(df3, names="categoria", values="total",
                      color_discrete_sequence=px.colors.qualitative.Set2,
                      hole=0.4)
        fig3.update_traces(textposition="inside", textinfo="percent+label+value")
        st.plotly_chart(fig3, use_container_width=True)
        st.caption(
            "Interpretacao: A maior parte das vendas e composta por veiculos USADOS, "
            "o que e tipico do mercado de leiloes norte-americano."
        )

with col4:
    st.subheader("Top 10 Marcas Mais Vendidas")
    sql4 = f"""
        SELECT v.marca,
               SUM(f.quantidade_vendida)    AS total,
               ROUND(AVG(f.preco_venda), 2) AS preco_medio
        FROM Fato_Vendas_Carros f
        JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
        JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
        JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
        WHERE {filtro_where()}
        GROUP BY v.marca
        ORDER BY total DESC
        LIMIT 10
    """
    df4 = rodar_query(sql4)
    if not df4.empty:
        fig4 = px.bar(df4, x="total", y="marca", orientation="h",
                      color="preco_medio", color_continuous_scale="Blues",
                      labels={"total": "Total Vendido", "marca": "Marca",
                              "preco_medio": "Preco Medio"},
                      text="total")
        fig4.update_layout(yaxis={"categoryorder": "total ascending"},
                           coloraxis_colorbar_title="Preco Medio (USD)")
        st.plotly_chart(fig4, use_container_width=True)
        st.caption(
            "Interpretacao: O gradiente de cor indica o ticket medio por marca. "
            "Marcas de luxo tendem a ter menos volume, mas maiores precos."
        )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 5 - Top 10 Precos por Categoria e Ano
# ---------------------------------------------------------------
st.subheader("Top 10 Maiores Precos de Veiculos por Categoria")

sql5 = f"""
    SELECT t.ano, v.categoria, v.marca, v.modelo, v.versao,
           ROUND(MAX(f.preco_venda), 2) AS preco_max,
           ROUND(AVG(f.preco_venda), 2) AS preco_medio
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
    GROUP BY t.ano, v.categoria, v.marca, v.modelo, v.versao
    ORDER BY preco_max DESC
    LIMIT 10
"""
df5 = rodar_query(sql5)
if not df5.empty:
    df5["veiculo"] = (
        df5["marca"] + " " + df5["modelo"] + " " +
        df5["versao"].fillna("") + " (" + df5["ano"].astype(str) + ")"
    )
    fig5 = px.bar(df5, x="preco_max", y="veiculo", orientation="h",
                  color="categoria",
                  labels={"preco_max": "Preco Maximo (USD)", "veiculo": "Veiculo",
                          "categoria": "Categoria"},
                  color_discrete_map={
                      "NOVO": "#2ca02c", "SEMINOVO": "#1f77b4",
                      "USADO": "#ff7f0e", "NAO INFORMADO": "#aec7e8"
                  },
                  text="preco_max")
    fig5.update_traces(texttemplate="$ %{x:,.0f}", textposition="outside")
    fig5.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig5, use_container_width=True)
    st.caption(
        "Interpretacao: Os veiculos de maior valor transacionado no periodo. "
        "Veiculos NOVOS e marcas premium frequentemente aparecem no topo da lista."
    )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 6 - Comparacao de Precos entre Lojas para o Mesmo Carro
# ---------------------------------------------------------------
st.subheader("Comparacao de Precos entre Lojas para o Mesmo Veiculo")

marcas_lista  = rodar_query("SELECT DISTINCT marca FROM Dim_Veiculo ORDER BY marca")["marca"].tolist()
col6a, col6b = st.columns(2)
with col6a:
    sel_marca_loja = st.selectbox("Marca", marcas_lista, key="marca_loja")
with col6b:
    modelos_lista = rodar_query(
        f"SELECT DISTINCT modelo FROM Dim_Veiculo WHERE marca = '{sel_marca_loja}' ORDER BY modelo"
    )["modelo"].tolist()
    sel_modelo_loja = st.selectbox("Modelo", modelos_lista, key="modelo_loja")

sql6 = f"""
    SELECT l.nome_loja,
           l.estado_loja,
           COUNT(*)                        AS total_vendas,
           ROUND(AVG(f.preco_venda), 2)    AS preco_medio_venda,
           ROUND(AVG(f.preco_mercado), 2)  AS preco_medio_mercado,
           ROUND(MIN(f.preco_venda), 2)    AS preco_minimo,
           ROUND(MAX(f.preco_venda), 2)    AS preco_maximo
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
      AND v.marca  = '{sel_marca_loja}'
      AND v.modelo = '{sel_modelo_loja}'
    GROUP BY l.nome_loja, l.estado_loja
    HAVING total_vendas >= 2
    ORDER BY preco_medio_venda DESC
    LIMIT 20
"""
df6 = rodar_query(sql6)
if not df6.empty:
    fig6 = go.Figure()
    fig6.add_trace(go.Bar(
        name="Preco Medio de Venda",
        x=df6["nome_loja"], y=df6["preco_medio_venda"],
        marker_color="steelblue",
        text=df6["preco_medio_venda"],
        texttemplate="$%{y:,.0f}", textposition="outside"
    ))
    fig6.add_trace(go.Bar(
        name="Preco de Mercado (MMR)",
        x=df6["nome_loja"], y=df6["preco_medio_mercado"],
        marker_color="salmon",
        text=df6["preco_medio_mercado"],
        texttemplate="$%{y:,.0f}", textposition="outside"
    ))
    fig6.update_layout(
        barmode="group",
        xaxis_title="Loja / Revendedor",
        yaxis_title="Preco Medio (USD)",
        xaxis_tickangle=-40,
        legend=dict(orientation="h", y=1.1),
        title=f"{sel_marca_loja} {sel_modelo_loja} - Preco por Loja"
    )
    st.plotly_chart(fig6, use_container_width=True)
    st.caption(
        "Interpretacao: Compara o preco medio praticado por cada loja para o mesmo modelo. "
        "Lojas com preco acima do MMR conseguem vender acima do valor de mercado, "
        "enquanto as abaixo do MMR oferecem maior desconto ao comprador."
    )
else:
    st.info("Nao ha dados suficientes para comparar lojas nessa combinacao de marca e modelo. Tente outro modelo.")

st.divider()


# ---------------------------------------------------------------
# GRAFICO 7 - Preco Venda vs Mercado por Marca
# ---------------------------------------------------------------
st.subheader("Comparacao: Preco de Venda vs Preco de Mercado - Top 10 Marcas")

sql7 = f"""
    SELECT v.marca,
           ROUND(AVG(f.preco_venda), 2)   AS avg_venda,
           ROUND(AVG(f.preco_mercado), 2) AS avg_mercado,
           ROUND(AVG(f.preco_venda - f.preco_mercado), 2) AS diferenca_media
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
    GROUP BY v.marca
    ORDER BY avg_venda DESC
    LIMIT 10
"""
df7 = rodar_query(sql7)
if not df7.empty:
    fig7 = go.Figure()
    fig7.add_trace(go.Bar(name="Preco de Venda (USD)",   x=df7["marca"], y=df7["avg_venda"],
                          marker_color="steelblue",
                          text=df7["avg_venda"], texttemplate="$%{y:,.0f}", textposition="outside"))
    fig7.add_trace(go.Bar(name="Preco de Mercado (MMR)", x=df7["marca"], y=df7["avg_mercado"],
                          marker_color="salmon",
                          text=df7["avg_mercado"], texttemplate="$%{y:,.0f}", textposition="outside"))
    fig7.update_layout(
        barmode="group",
        xaxis_title="Marca", yaxis_title="Preco Medio (USD)",
        legend=dict(orientation="h", y=1.12),
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig7, use_container_width=True)
    st.caption(
        "Interpretacao: Quando o preco de venda fica abaixo do MMR, a loja negociou com desconto. "
        "Acima do MMR pode indicar veiculos muito demandados ou com opcionais valorizados."
    )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 8 - Mapa por Estado (EUA)
# ---------------------------------------------------------------
st.subheader("Distribuicao de Vendas por Estado (EUA)")

sql8 = f"""
    SELECT l.estado_loja, l.nome_estado_loja, l.regiao_loja,
           COUNT(*)                      AS total_vendas,
           ROUND(SUM(f.preco_venda), 2) AS receita_total,
           ROUND(AVG(f.preco_venda), 2) AS ticket_medio
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    WHERE {filtro_where()} AND LENGTH(l.estado_loja) = 2
    GROUP BY l.estado_loja, l.nome_estado_loja, l.regiao_loja
    ORDER BY total_vendas DESC
"""
df8 = rodar_query(sql8)
metrica_mapa = st.radio(
    "Metrica:", ["total_vendas", "receita_total", "ticket_medio"],
    horizontal=True,
    format_func=lambda x: {
        "total_vendas":  "Total de Vendas",
        "receita_total": "Receita Total",
        "ticket_medio":  "Ticket Medio"
    }[x]
)
if not df8.empty:
    fig8 = px.choropleth(
        df8,
        locations="estado_loja",
        locationmode="USA-states",
        color=metrica_mapa,
        scope="usa",
        hover_name="nome_estado_loja",
        hover_data={"estado_loja": False, "total_vendas": True,
                    "receita_total": True, "ticket_medio": True},
        color_continuous_scale="Blues",
        labels={
            "total_vendas":  "Total de Vendas",
            "receita_total": "Receita (USD)",
            "ticket_medio":  "Ticket Medio (USD)",
        },
    )
    fig8.update_layout(geo=dict(showlakes=True, lakecolor="rgb(255,255,255)"))
    st.plotly_chart(fig8, use_container_width=True)
    st.caption(
        "Interpretacao: Estados com maior concentracao de vendas sao os grandes centros "
        "populacionais (CA, TX, FL). O ticket medio revela diferencas de poder aquisitivo "
        "entre regioes."
    )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 9 - Top 10 Lojas por Volume
# ---------------------------------------------------------------
st.subheader("Top 10 Lojas / Revendedores por Volume de Vendas")

sql9 = f"""
    SELECT l.nome_loja, l.estado_loja, l.regiao_loja,
           COUNT(*)                      AS total_vendas,
           ROUND(SUM(f.preco_venda), 2) AS receita_total,
           ROUND(AVG(f.preco_venda), 2) AS ticket_medio
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    WHERE {filtro_where()}
    GROUP BY l.nome_loja, l.estado_loja, l.regiao_loja
    ORDER BY total_vendas DESC
    LIMIT 10
"""
df9 = rodar_query(sql9)
if not df9.empty:
    fig9 = px.bar(
        df9, x="total_vendas", y="nome_loja", orientation="h",
        color="ticket_medio", color_continuous_scale="Viridis",
        labels={"total_vendas": "Total Vendido", "nome_loja": "Loja/Revendedor",
                "ticket_medio": "Ticket Medio (USD)"},
        text="total_vendas",
    )
    fig9.update_layout(yaxis={"categoryorder": "total ascending"},
                       coloraxis_colorbar_title="Ticket Medio")
    st.plotly_chart(fig9, use_container_width=True)
    st.caption(
        "Interpretacao: Alto volume com baixo ticket indica foco em veiculos populares. "
        "Baixo volume com alto ticket sugere especializacao em segmento premium."
    )

st.divider()


# ---------------------------------------------------------------
# GRAFICO 10 - Serie Temporal por Marca e Modelo
# ---------------------------------------------------------------
st.subheader("Evolucao de Preco por Marca e Modelo ao Longo do Tempo")

marcas_serie = rodar_query("SELECT DISTINCT marca FROM Dim_Veiculo ORDER BY marca")["marca"].tolist()
col10a, col10b = st.columns(2)
with col10a:
    sel_marca_serie = st.selectbox("Marca", marcas_serie, key="marca_serie")
with col10b:
    modelos_serie = rodar_query(
        f"SELECT DISTINCT modelo FROM Dim_Veiculo WHERE marca = '{sel_marca_serie}' ORDER BY modelo"
    )["modelo"].tolist()
    sel_modelo_serie = st.selectbox("Modelo", modelos_serie, key="modelo_serie")

sql10 = f"""
    SELECT t.ano, t.numero_mes,
           ROUND(AVG(f.preco_venda), 2)   AS preco_medio_venda,
           ROUND(AVG(f.preco_mercado), 2) AS preco_medio_mercado,
           COUNT(*) AS total_vendas
    FROM Fato_Vendas_Carros f
    JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
    JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
    JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
    WHERE {filtro_where()}
      AND v.marca  = '{sel_marca_serie}'
      AND v.modelo = '{sel_modelo_serie}'
    GROUP BY t.ano, t.numero_mes
    ORDER BY t.ano, t.numero_mes
"""
df10 = rodar_query(sql10)
if not df10.empty and len(df10) > 1:
    df10["periodo"] = df10["ano"].astype(str) + "-" + df10["numero_mes"].astype(str).str.zfill(2)
    fig10 = go.Figure()
    fig10.add_trace(go.Scatter(x=df10["periodo"], y=df10["preco_medio_venda"],
                               mode="lines+markers", name="Preco de Venda",
                               fill="tozeroy", fillcolor="rgba(31,119,180,0.1)",
                               line=dict(color="#1f77b4")))
    fig10.add_trace(go.Scatter(x=df10["periodo"], y=df10["preco_medio_mercado"],
                               mode="lines+markers", name="Preco MMR",
                               line=dict(color="#ff7f0e", dash="dash")))
    fig10.update_layout(
        title=f"{sel_marca_serie} {sel_modelo_serie} - Evolucao de Preco",
        xaxis_title="Periodo", yaxis_title="Preco Medio (USD)",
        xaxis_tickangle=-45, hovermode="x unified",
    )
    st.plotly_chart(fig10, use_container_width=True)
    st.caption(
        f"Interpretacao: Serie temporal de preco para {sel_marca_serie} {sel_modelo_serie}. "
        "Quedas acentuadas podem indicar desvalorizacao sazonal, enquanto picos sugerem "
        "alta demanda ou lotes especiais naquele periodo."
    )
elif not df10.empty:
    st.info("Dados insuficientes para gerar serie temporal. Tente ajustar os filtros.")
else:
    st.warning("Nenhum dado encontrado para essa combinacao.")

st.divider()


# ---------------------------------------------------------------
# TABELA - Amostra dos dados do DW
# ---------------------------------------------------------------
with st.expander("Ver amostra dos dados do Data Warehouse (top 20)"):
    sql_tabela = f"""
        SELECT
            t.data_completa, t.ano, t.trimestre, t.semestre,
            v.marca, v.modelo, v.versao, v.categoria, v.transmissao,
            v.cor_externa, v.faixa_idade_veiculo,
            l.nome_loja, l.estado_loja, l.regiao_loja,
            f.preco_venda, f.preco_mercado,
            ROUND(f.preco_venda - f.preco_mercado, 2) AS diferenca
        FROM Fato_Vendas_Carros f
        JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
        JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
        JOIN Dim_Loja_Venda  l ON f.sk_loja        = l.sk_loja
        WHERE {filtro_where()}
        ORDER BY f.preco_venda DESC
        LIMIT 20
    """
    df_tabela = rodar_query(sql_tabela)
    st.dataframe(df_tabela, use_container_width=True)


st.markdown("---")
st.caption(
    "AutoPrime BI | Laboratorio de Banco de Dados - GP0029VNO05A | "
    "Prof. Anderson Barroso | Universidade Tiradentes"
)
