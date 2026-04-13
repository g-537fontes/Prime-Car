# 🚗 AutoPrime — Projeto Data Warehouse

**Disciplina:** Laboratório de Banco de Dados — GP0029VNO05A  
**Professor:** Anderson Barroso  
**Curso:** Ciências da Computação — 5º Período  
**Universidade Tiradentes**  
**Data de Apresentação:** 13/04/2026

---

## 📁 Estrutura de Arquivos

```
autoprime/
├── etl.py                  # Pipeline ETL completo (Extração → Transformação → Carga)
├── dashboard.py            # Dashboard BI interativo (Streamlit + Plotly)
├── create_dw_mysql.sql     # DDL do Data Warehouse (MySQL 8.x + dados amostrais + views)
├── requirements.txt        # Dependências Python
└── README.md               # Este arquivo
```

O arquivo `dw_autoprime.db` é gerado automaticamente pelo `etl.py`.

---

## ⚙️ Instalação e Execução

### 1. Pré-requisitos
- Python 3.11+ instalado
- Acesso à internet (para conectar ao MySQL Aiven Cloud)

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Executar o Pipeline ETL

```bash
python etl.py
```

Isso irá:
- Conectar ao banco MySQL no Aiven Cloud
- Extrair os dados da tabela `prime_vendas.vendas` (anos 2000–2014)
- Aplicar todas as transformações e validações de qualidade
- Gerar o arquivo `dw_autoprime.db` (SQLite local) com o esquema estrela populado

### 4. Executar o Dashboard

```bash
streamlit run dashboard.py
```

O browser abrirá automaticamente em `http://localhost:8501`

---

## 🏗️ Modelagem Dimensional — Esquema Estrela

```
                    ┌──────────────────────────┐
                    │     Dim_Tempo_Venda       │
                    │─────────────────────────  │
                    │ PK  sk_tempo              │
                    │     data_completa         │
                    │     ano / mes / dia       │
                    │     trimestre / semestre  │
                    │     nome_mes / numero_mes │
                    │     dia_semana            │
                    │     indicador_fim_semana  │
                    └────────────┬─────────────┘
                                 │
┌─────────────────────┐          │          ┌──────────────────────────┐
│    Dim_Veiculo      │          │          │    Dim_Loja_Venda         │
│─────────────────────│          │          │──────────────────────────│
│ PK  sk_veiculo      │          │          │ PK  sk_loja              │
│     ano_fabricacao  │          │          │     nome_loja            │
│     marca           │          │          │     estado_loja          │
│     modelo          │          │          │     nome_estado_loja     │
│     versao          │          │          │     regiao_loja          │
│     tipo_carroceria │          │          └────────────┬─────────────┘
│     chassi          │          │                       │
│     idade_veiculo   │          │                       │
│     faixa_idade     │          │                       │
│     cor_interna     │    ┌─────▼───────────────────────▼──────┐
│     cor_externa     │    │        Fato_Vendas_Carros           │
│     odometro        │    │────────────────────────────────────│
│     categoria       │    │ PK  id_fato                        │
│     transmissao     │◄───│ FK  sk_tempo_venda                 │
└─────────────────────┘    │ FK  sk_veiculo                     │
                           │ FK  sk_loja                        │
                           │     quantidade_vendida  = 1        │
                           │     preco_venda  (sellingprice)    │
                           │     preco_mercado (mmr)            │
                           └────────────────────────────────────┘
```

---

## 🔄 Fases do Pipeline ETL

### E — Extração
- Conexão ao MySQL hospedado no Aiven Cloud
- Query com filtro `WHERE year BETWEEN 2000 AND 2014`
- Dados classificados como camada **BRONZE**

### T — Transformação

| Regra | Descrição |
|-------|-----------|
| **Marca faltando** | Preenchida com `"NAO INFORMADO"` |
| **Transmissão** | Padronizada para `AUTOMATICO`, `MANUAL` ou `NAO INFORMADO` |
| **Estado → Nome + Região** | Mapeamento completo dos 50 estados + DC |
| **Parse de Data** | Converte `"Tue Dec 16 2014 12:30:00 GMT0800 (PST)"` → `datetime` |
| **Campos temporais** | Deriva ano, mês, dia, trimestre, semestre, dia da semana, fim de semana |
| **Categoria do veículo** | NOVO (≤5.000 mi e ≤1 ano) / SEMINOVO (≤30.000 mi ou ≤3 anos) / USADO |
| **Faixa de odômetro** | Agrupa em 5 faixas (0–15k / 15–30k / 30–60k / 60–100k / +100k milhas) |
| **Padronização de texto** | Maiúsculo, sem acentos, sem espaços duplos, sem caracteres estranhos |
| **Deduplicação de lojas** | Normaliza nome antes de comparar (evita `"MANHEIM TX"` vs `"MANHEIM TX "`) |
| **Preço ≤ 0** | Registros descartados |
| **MMR ≤ 0** | Registros descartados |
| **Odômetro negativo** | Registros descartados |
| **Outliers de preço** | Descarta registros onde `preco_venda / mmr < 0.20` ou `> 5.00` |
| **Data inválida** | Registros com data não parseável são descartados |

### L — Carga
- Carga em banco SQLite local (`dw_autoprime.db`)
- Modo idempotente: trunca e reinsere a cada execução
- Ordem de carga respeita integridade referencial:  
  `Dim_Tempo → Dim_Veiculo → Dim_Loja → Fato`

---

## 📊 Visualizações do Dashboard

| # | Gráfico | Pergunta de Negócio Respondida |
|---|---------|-------------------------------|
| 1 | Linha — Evolução de Preço Médio | Como o preço médio de venda evoluiu ao longo do tempo? |
| 2 | Histograma — Distribuição de Preço | Qual é a concentração de vendas por faixa de preço? |
| 3 | Pizza — Categorias Mais Ofertadas | Qual a proporção entre Novo, Seminovo e Usado? |
| 4 | Barra horizontal — Top 10 Marcas | Quais marcas têm maior volume de vendas? |
| 5 | Barra horizontal — Top 10 Preços | Quais veículos alcançaram os maiores preços? |
| 6 | Barras agrupadas — Preço Venda vs Mercado | As lojas vendem acima ou abaixo do preço de mercado? |
| 7 | Mapa Choropleth (EUA) | Como as vendas se distribuem geograficamente? |
| 8 | Barra horizontal — Top 10 Lojas | Quais revendedores têm maior volume e ticket médio? |
| 9 | Série temporal — Marca/Modelo | Como o preço de um modelo específico variou ao longo do tempo? |

**Filtros disponíveis:** Ano · Mês · Trimestre · Semestre · Região

---

## 🗄️ Script SQL (`create_dw_mysql.sql`)

O script contém:
- `CREATE DATABASE dw_autoprime`
- DDL das 3 dimensões e da tabela fato com comentários
- Índices de performance
- 5 registros de dados amostrais em cada tabela
- 3 views analíticas prontas para uso:
  - `vw_vendas_por_marca_ano`
  - `vw_desempenho_por_loja`
  - `vw_diferenca_preco`

---

## 📌 Observações Técnicas

- O DW local usa **SQLite** para facilitar a execução sem servidor.  
  Para usar **MySQL**, aplique `create_dw_mysql.sql` e altere a string de conexão em `etl.py`.
- Todos os textos são armazenados em **maiúsculo sem acentuação** no DW.
- O campo `quantidade_vendida` é sempre `1` (granularidade: 1 linha = 1 venda).
- O pipeline é **idempotente**: pode ser re-executado sem gerar duplicatas.
