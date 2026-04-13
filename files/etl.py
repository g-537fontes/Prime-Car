import re
import sqlite3
import unicodedata
import warnings
from datetime import datetime

import mysql.connector
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# Credenciais do banco de dados de origem
SOURCE_CONFIG = {
    "user":             "consulta",
    "password":         "AVNS_DPtGCiuDuKw46LcRCjB",
    "host":             "mysql-3fa4fc41-giga-d6d4.l.aivencloud.com",
    "port":             13729,
    "database":         "prime_vendas",
    "ssl_disabled":     False,
    "connection_timeout": 30,
}
TABLE_NAME = "vendas"

# Caminho do Data Warehouse local
DW_PATH = "dw_autoprime.db"

# Mapeamento de sigla de estado para nome completo e regiao
ESTADOS_USA = {
    "AL": ("ALABAMA",              "SUL"),
    "AK": ("ALASKA",               "OESTE"),
    "AZ": ("ARIZONA",              "OESTE"),
    "AR": ("ARKANSAS",             "SUL"),
    "CA": ("CALIFORNIA",           "OESTE"),
    "CO": ("COLORADO",             "OESTE"),
    "CT": ("CONNECTICUT",          "NORDESTE"),
    "DE": ("DELAWARE",             "SUL"),
    "FL": ("FLORIDA",              "SUL"),
    "GA": ("GEORGIA",              "SUL"),
    "HI": ("HAWAII",               "OESTE"),
    "ID": ("IDAHO",                "OESTE"),
    "IL": ("ILLINOIS",             "CENTRO-OESTE"),
    "IN": ("INDIANA",              "CENTRO-OESTE"),
    "IA": ("IOWA",                 "CENTRO-OESTE"),
    "KS": ("KANSAS",               "CENTRO-OESTE"),
    "KY": ("KENTUCKY",             "SUL"),
    "LA": ("LOUISIANA",            "SUL"),
    "ME": ("MAINE",                "NORDESTE"),
    "MD": ("MARYLAND",             "SUL"),
    "MA": ("MASSACHUSETTS",        "NORDESTE"),
    "MI": ("MICHIGAN",             "CENTRO-OESTE"),
    "MN": ("MINNESOTA",            "CENTRO-OESTE"),
    "MS": ("MISSISSIPPI",          "SUL"),
    "MO": ("MISSOURI",             "CENTRO-OESTE"),
    "MT": ("MONTANA",              "OESTE"),
    "NE": ("NEBRASKA",             "CENTRO-OESTE"),
    "NV": ("NEVADA",               "OESTE"),
    "NH": ("NEW HAMPSHIRE",        "NORDESTE"),
    "NJ": ("NEW JERSEY",           "NORDESTE"),
    "NM": ("NEW MEXICO",           "OESTE"),
    "NY": ("NEW YORK",             "NORDESTE"),
    "NC": ("NORTH CAROLINA",       "SUL"),
    "ND": ("NORTH DAKOTA",         "CENTRO-OESTE"),
    "OH": ("OHIO",                 "CENTRO-OESTE"),
    "OK": ("OKLAHOMA",             "SUL"),
    "OR": ("OREGON",               "OESTE"),
    "PA": ("PENNSYLVANIA",         "NORDESTE"),
    "RI": ("RHODE ISLAND",         "NORDESTE"),
    "SC": ("SOUTH CAROLINA",       "SUL"),
    "SD": ("SOUTH DAKOTA",         "CENTRO-OESTE"),
    "TN": ("TENNESSEE",            "SUL"),
    "TX": ("TEXAS",                "SUL"),
    "UT": ("UTAH",                 "OESTE"),
    "VT": ("VERMONT",              "NORDESTE"),
    "VA": ("VIRGINIA",             "SUL"),
    "WA": ("WASHINGTON",           "OESTE"),
    "WV": ("WEST VIRGINIA",        "SUL"),
    "WI": ("WISCONSIN",            "CENTRO-OESTE"),
    "WY": ("WYOMING",              "OESTE"),
    "DC": ("DISTRITO DE COLUMBIA", "SUL"),
    "PR": ("PORTO RICO",           "NAO IDENTIFICADO"),
    "QC": ("QUEBEC",               "NAO IDENTIFICADO"),
    "ON": ("ONTARIO",              "NAO IDENTIFICADO"),
    "AB": ("ALBERTA",              "NAO IDENTIFICADO"),
    "NS": ("NOVA ESCOCIA",         "NAO IDENTIFICADO"),
    "BC": ("BRITISH COLUMBIA",     "NAO IDENTIFICADO"),
}


def remover_acentos(texto):
    nfkd = unicodedata.normalize("NFKD", str(texto))
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def limpar_texto(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "NAO INFORMADO"
    texto = str(valor).strip()
    if not texto or texto.lower() in ("nan", "none", "null", "n/a", "-"):
        return "NAO INFORMADO"
    texto = remover_acentos(texto)
    texto = re.sub(r"\s+", " ", texto)
    texto = re.sub(r"[^\w\s\-\/\.\,]", "", texto)
    texto = texto.upper().strip()
    return texto if texto else "NAO INFORMADO"


def parse_saledate(date_str):
    if date_str is None or (isinstance(date_str, float) and np.isnan(date_str)):
        return None
    s = str(date_str).strip()
    s = re.sub(r"\s*\([^)]*\)", "", s).strip()
    s = re.sub(r"\s*GMT[+-]?\d*", "", s).strip()
    formatos = [
        "%a %b %d %Y %H:%M:%S",
        "%a %b  %d %Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]
    for fmt in formatos:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        result = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(result) else result.to_pydatetime()
    except Exception:
        return None


def padronizar_transmissao(valor):
    if valor is None or (isinstance(valor, float) and np.isnan(valor)):
        return "NAO INFORMADO"
    v = str(valor).strip().upper()
    if not v or v in ("NAN", "NONE", "NULL", "N/A"):
        return "NAO INFORMADO"
    if any(k in v for k in ("AUTO", "AT", "CVT", "DCT")):
        return "AUTOMATICO"
    if any(k in v for k in ("MANUAL", "MAN", "MT")) or v == "M":
        return "MANUAL"
    return "NAO INFORMADO"


def classificar_categoria(odometro, ano_fabricacao, ano_venda):
    try:
        km    = float(odometro)
        idade = int(ano_venda) - int(ano_fabricacao)
    except (TypeError, ValueError):
        return "NAO INFORMADO"
    if km <= 5000 and idade <= 1:
        return "NOVO"
    if km <= 30000 or idade <= 3:
        return "SEMINOVO"
    return "USADO"


def classificar_faixa_odometro(odometro):
    try:
        km = float(odometro)
    except (TypeError, ValueError):
        return "NAO INFORMADO"
    if km <= 15000:
        return "0 - 15.000 MI"
    if km <= 30000:
        return "15.001 - 30.000 MI"
    if km <= 60000:
        return "30.001 - 60.000 MI"
    if km <= 100000:
        return "60.001 - 100.000 MI"
    return "ACIMA DE 100.000 MI"


def get_estado_info(sigla):
    sigla = str(sigla).strip().upper()
    return ESTADOS_USA.get(sigla, ("NAO IDENTIFICADO", "NAO IDENTIFICADO"))


# ---------------------------------------------------------------
# EXTRACAO
# ---------------------------------------------------------------
def extrair_dados():
    print("\nIniciando extracao dos dados...")

    try:
        conn = mysql.connector.connect(**SOURCE_CONFIG)
    except mysql.connector.Error as e:
        print(f"Erro ao conectar: {e}")
        raise

    query = f"SELECT * FROM {TABLE_NAME}"
    print("Executando query na tabela de origem...")
    df = pd.read_sql(query, conn)
    conn.close()

    print(f"{len(df):,} registros extraidos.")
    print(f"Colunas: {list(df.columns)}")
    return df


# ---------------------------------------------------------------
# TRANSFORMACAO
# ---------------------------------------------------------------
def transformar_dados(df):
    print("\nIniciando transformacao dos dados...")
    total_original = len(df)

    # Validacoes de qualidade
    print("Aplicando validacoes de qualidade...")
    mask_pv  = pd.to_numeric(df["sellingprice"], errors="coerce") > 0
    mask_mmr = pd.to_numeric(df["mmr"],          errors="coerce") > 0
    mask_odo = pd.to_numeric(df["odometer"],     errors="coerce") >= 0
    mask_ano = pd.to_numeric(df["ano"],          errors="coerce").between(2000, 2014)

    df = df[mask_pv & mask_mmr & mask_odo & mask_ano].copy()

    df["sellingprice"] = pd.to_numeric(df["sellingprice"], errors="coerce")
    df["mmr"]          = pd.to_numeric(df["mmr"],          errors="coerce")
    df["odometer"]     = pd.to_numeric(df["odometer"],     errors="coerce")
    df["ano"]          = pd.to_numeric(df["ano"],          errors="coerce").astype("Int64")

    # Remove outliers de preco
    ratio = df["sellingprice"] / df["mmr"]
    df = df[(ratio >= 0.20) & (ratio <= 5.00)].copy()

    removidos = total_original - len(df)
    print(f"{removidos:,} registros removidos por qualidade.")
    print(f"{len(df):,} registros validos.")

    # Parse da data de venda
    print("Convertendo datas...")
    df["_dt_venda"] = df["saledate"].apply(parse_saledate)
    antes = len(df)
    df = df[df["_dt_venda"].notna()].copy()
    df["_dt_venda"] = pd.to_datetime(df["_dt_venda"])
    print(f"{antes - len(df):,} registros com data invalida descartados.")

    # Campos de tempo derivados
    print("Criando campos de tempo...")
    df["_ano"]        = df["_dt_venda"].dt.year
    df["_mes"]        = df["_dt_venda"].dt.month
    df["_dia"]        = df["_dt_venda"].dt.day
    df["_trimestre"]  = df["_dt_venda"].dt.quarter
    df["_semestre"]   = df["_mes"].apply(lambda m: 1 if m <= 6 else 2)
    df["_nome_mes"]   = df["_dt_venda"].dt.strftime("%B").str.upper()
    df["_dia_semana"] = df["_dt_venda"].dt.strftime("%A").str.upper()
    df["_fim_semana"] = df["_dt_venda"].dt.dayofweek.apply(
        lambda d: "SIM" if d >= 5 else "NAO"
    )

    # Padronizacao de textos
    print("Padronizando textos...")
    colunas_texto = ["make", "model", "trim_veiculo", "body",
                     "color", "interior", "seller", "vin"]
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].apply(limpar_texto)

    if "transmission" in df.columns:
        df["transmission"] = df["transmission"].apply(padronizar_transmissao)

    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip().str.upper()

    # Enriquecimento de estado
    print("Enriquecendo dados de estado...")
    df["_nome_estado"] = df["state"].apply(lambda s: get_estado_info(s)[0])
    df["_regiao"]      = df["state"].apply(lambda s: get_estado_info(s)[1])

    # Categoria e faixa do veiculo
    print("Classificando categoria dos veiculos...")
    df["_categoria"]      = df.apply(
        lambda r: classificar_categoria(r["odometer"], r["ano"], r["_ano"]), axis=1
    )
    df["_faixa_odometro"] = df["odometer"].apply(classificar_faixa_odometro)
    df["_idade_veiculo"]  = df["_ano"] - df["ano"].fillna(0).astype(int)

    # Dim_Tempo_Venda
    print("Construindo Dim_Tempo_Venda...")
    dim_tempo = (
        df[["_dt_venda", "_ano", "_mes", "_dia",
            "_trimestre", "_semestre", "_nome_mes",
            "_dia_semana", "_fim_semana"]]
        .drop_duplicates(subset=["_dt_venda"])
        .copy()
        .reset_index(drop=True)
    )
    dim_tempo.columns = [
        "data_completa", "ano", "mes", "dia",
        "trimestre", "semestre", "nome_mes",
        "dia_semana", "indicador_fim_semana",
    ]
    dim_tempo["numero_mes"]    = dim_tempo["mes"]
    dim_tempo["sk_tempo"]      = range(1, len(dim_tempo) + 1)
    dim_tempo["data_completa"] = dim_tempo["data_completa"].dt.strftime("%Y-%m-%d")

    # Dim_Veiculo
    print("Construindo Dim_Veiculo...")
    df["vin"] = df["vin"].replace("NAO INFORMADO", np.nan)
    df["_vin_key"] = df["vin"].fillna(
        df["make"].astype(str) + "_" +
        df["model"].astype(str) + "_" +
        df["ano"].astype(str) + "_" +
        df["odometer"].astype(str)
    )

    veiculo_raw = df[[
        "_vin_key", "ano", "make", "model", "trim_veiculo", "body",
        "vin", "color", "interior", "odometer",
        "transmission", "_categoria", "_faixa_odometro", "_idade_veiculo"
    ]].copy()

    dim_veiculo = (
        veiculo_raw
        .drop_duplicates(subset=["_vin_key"])
        .reset_index(drop=True)
    )
    dim_veiculo.rename(columns={
        "ano":             "ano_fabricacao",
        "make":            "marca",
        "model":           "modelo",
        "trim_veiculo":    "versao",
        "body":            "tipo_carroceria",
        "vin":             "chassi",
        "color":           "cor_externa",
        "interior":        "cor_interna",
        "odometer":        "odometro",
        "transmission":    "transmissao",
        "_categoria":      "categoria",
        "_faixa_odometro": "faixa_idade_veiculo",
        "_idade_veiculo":  "idade_veiculo_no_momento_da_venda",
    }, inplace=True)
    dim_veiculo["sk_veiculo"] = range(1, len(dim_veiculo) + 1)

    # Dim_Loja_Venda
    print("Construindo Dim_Loja_Venda...")
    loja_raw = df[["seller", "state", "_nome_estado", "_regiao"]].copy()
    loja_raw["_seller_norm"] = loja_raw["seller"].apply(
        lambda s: re.sub(r"\s+", " ", str(s)).strip()
    )
    dim_loja = (
        loja_raw
        .drop_duplicates(subset=["_seller_norm"])
        .reset_index(drop=True)
    )
    dim_loja = dim_loja.rename(columns={
        "seller":       "nome_loja",
        "state":        "estado_loja",
        "_nome_estado": "nome_estado_loja",
        "_regiao":      "regiao_loja",
    })[["nome_loja", "estado_loja", "nome_estado_loja", "regiao_loja"]].copy()
    dim_loja["sk_loja"] = range(1, len(dim_loja) + 1)

    # Fato_Vendas_Carros
    print("Construindo Fato_Vendas_Carros...")
    mapa_tempo   = dim_tempo.set_index("data_completa")["sk_tempo"].to_dict()
    mapa_veiculo = dim_veiculo.set_index("_vin_key")["sk_veiculo"].to_dict()
    mapa_loja    = dim_loja.set_index("nome_loja")["sk_loja"].to_dict()

    fato = df.copy()
    fato["_dt_str"]        = fato["_dt_venda"].dt.strftime("%Y-%m-%d")
    fato["sk_tempo_venda"] = fato["_dt_str"].map(mapa_tempo)
    fato["sk_veiculo"]     = fato["_vin_key"].map(mapa_veiculo)
    fato["sk_loja"]        = fato["seller"].map(mapa_loja)
    fato["quantidade_vendida"] = 1

    fato_final = fato[[
        "sk_tempo_venda", "sk_veiculo", "sk_loja",
        "quantidade_vendida", "sellingprice", "mmr"
    ]].copy()
    fato_final.columns = [
        "sk_tempo_venda", "sk_veiculo", "sk_loja",
        "quantidade_vendida", "preco_venda", "preco_mercado",
    ]
    fato_final = fato_final.dropna(
        subset=["sk_tempo_venda", "sk_veiculo", "sk_loja"]
    ).reset_index(drop=True)
    fato_final["sk_tempo_venda"] = fato_final["sk_tempo_venda"].astype(int)
    fato_final["sk_veiculo"]     = fato_final["sk_veiculo"].astype(int)
    fato_final["sk_loja"]        = fato_final["sk_loja"].astype(int)

    dim_veiculo.drop(columns=["_vin_key"], inplace=True, errors="ignore")

    print(f"\nResumo da transformacao:")
    print(f"  Fato_Vendas_Carros : {len(fato_final):,} registros")
    print(f"  Dim_Tempo_Venda    : {len(dim_tempo):,} registros")
    print(f"  Dim_Veiculo        : {len(dim_veiculo):,} registros")
    print(f"  Dim_Loja_Venda     : {len(dim_loja):,} registros")

    return {
        "fato":        fato_final,
        "dim_tempo":   dim_tempo,
        "dim_veiculo": dim_veiculo,
        "dim_loja":    dim_loja,
    }


# ---------------------------------------------------------------
# CARGA
# ---------------------------------------------------------------
DDL_DW = """
CREATE TABLE IF NOT EXISTS Dim_Tempo_Venda (
    sk_tempo                INTEGER PRIMARY KEY,
    data_completa           TEXT    NOT NULL,
    ano                     INTEGER NOT NULL,
    mes                     INTEGER NOT NULL,
    nome_mes                TEXT    NOT NULL,
    numero_mes              INTEGER NOT NULL,
    dia                     INTEGER NOT NULL,
    trimestre               INTEGER NOT NULL,
    semestre                INTEGER NOT NULL,
    dia_semana              TEXT    NOT NULL,
    indicador_fim_semana    TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Veiculo (
    sk_veiculo                          INTEGER PRIMARY KEY,
    ano_fabricacao                      INTEGER NOT NULL,
    marca                               TEXT    NOT NULL,
    modelo                              TEXT    NOT NULL,
    versao                              TEXT,
    tipo_carroceria                     TEXT,
    chassi                              TEXT,
    idade_veiculo_no_momento_da_venda   INTEGER,
    faixa_idade_veiculo                 TEXT,
    cor_interna                         TEXT,
    cor_externa                         TEXT,
    odometro                            REAL,
    categoria                           TEXT    NOT NULL,
    transmissao                         TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Loja_Venda (
    sk_loja             INTEGER PRIMARY KEY,
    nome_loja           TEXT NOT NULL,
    estado_loja         TEXT NOT NULL,
    nome_estado_loja    TEXT NOT NULL,
    regiao_loja         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS Fato_Vendas_Carros (
    id_fato             INTEGER PRIMARY KEY AUTOINCREMENT,
    sk_tempo_venda      INTEGER NOT NULL,
    sk_veiculo          INTEGER NOT NULL,
    sk_loja             INTEGER NOT NULL,
    quantidade_vendida  INTEGER DEFAULT 1,
    preco_venda         REAL    NOT NULL,
    preco_mercado       REAL    NOT NULL,
    FOREIGN KEY (sk_tempo_venda) REFERENCES Dim_Tempo_Venda(sk_tempo),
    FOREIGN KEY (sk_veiculo)     REFERENCES Dim_Veiculo(sk_veiculo),
    FOREIGN KEY (sk_loja)        REFERENCES Dim_Loja_Venda(sk_loja)
);

CREATE INDEX IF NOT EXISTS idx_fato_tempo   ON Fato_Vendas_Carros(sk_tempo_venda);
CREATE INDEX IF NOT EXISTS idx_fato_veiculo ON Fato_Vendas_Carros(sk_veiculo);
CREATE INDEX IF NOT EXISTS idx_fato_loja    ON Fato_Vendas_Carros(sk_loja);
"""


def criar_dw(conn):
    conn.executescript(DDL_DW)
    conn.commit()


def carregar_dados(dados):
    print(f"\nCarregando dados no DW: {DW_PATH}")

    conn = sqlite3.connect(DW_PATH)
    criar_dw(conn)

    tabelas = [
        "Fato_Vendas_Carros",
        "Dim_Loja_Venda",
        "Dim_Veiculo",
        "Dim_Tempo_Venda",
    ]
    cur = conn.cursor()
    for t in tabelas:
        cur.execute(f"DELETE FROM {t}")
    conn.commit()

    mapa_carga = [
        ("Dim_Tempo_Venda",    dados["dim_tempo"]),
        ("Dim_Veiculo",        dados["dim_veiculo"]),
        ("Dim_Loja_Venda",     dados["dim_loja"]),
        ("Fato_Vendas_Carros", dados["fato"]),
    ]

    for nome_tabela, df_carga in mapa_carga:
        print(f"Carregando {nome_tabela} ({len(df_carga):,} linhas)...")
        df_carga.to_sql(
            nome_tabela, conn,
            if_exists="append",
            index=False,
            chunksize=10000,
        )

    conn.close()
    print("\nCarga concluida. DW disponivel em:", DW_PATH)


# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------
def main():
    print("AutoPrime - Pipeline ETL")
    print("Disciplina: Laboratorio de Banco de Dados")
    print("Prof. Anderson Barroso\n")

    df_raw = extrair_dados()
    dados_transformados = transformar_dados(df_raw)
    carregar_dados(dados_transformados)

    print("\nETL finalizado. Execute o dashboard com:")
    print("py -3.11 -m streamlit run dashboard.py")


if __name__ == "__main__":
    main()
