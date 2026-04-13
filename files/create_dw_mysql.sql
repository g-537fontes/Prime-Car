-- ==============================================================
--  AUTOPRIME — DATA WAREHOUSE
--  Script DDL de Criação — Compatível com MySQL 8.x
--  Disciplina: Laboratório de Banco de Dados
--  Prof. Anderson Barroso
-- ==============================================================
--
--  Modelo: Esquema Estrela (Star Schema)
--
--  Tabela Fato  : Fato_Vendas_Carros
--  Dimensões    : Dim_Tempo_Venda | Dim_Veiculo | Dim_Loja_Venda
--
--  Como executar no MySQL:
--    mysql -u <usuario> -p < create_dw_mysql.sql
--  Ou via MySQL Workbench: abra e execute este arquivo.
-- ==============================================================

-- --------------------------------------------------------------
-- BANCO DE DADOS
-- --------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS dw_autoprime
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE dw_autoprime;


-- ==============================================================
-- DIMENSÃO 1 — Dim_Tempo_Venda
-- ==============================================================
CREATE TABLE IF NOT EXISTS Dim_Tempo_Venda (
    sk_tempo                INT            NOT NULL COMMENT 'Surrogate Key — chave substituta da dimensão tempo',
    data_completa           DATE           NOT NULL COMMENT 'Data completa da venda (YYYY-MM-DD)',
    ano                     SMALLINT       NOT NULL COMMENT 'Ano da venda (ex: 2014)',
    mes                     TINYINT        NOT NULL COMMENT 'Número do mês (1 a 12)',
    nome_mes                VARCHAR(20)    NOT NULL COMMENT 'Nome do mês em inglês maiúsculo (ex: JANUARY)',
    numero_mes              TINYINT        NOT NULL COMMENT 'Número do mês — redundante, facilita consultas',
    dia                     TINYINT        NOT NULL COMMENT 'Dia do mês (1 a 31)',
    trimestre               TINYINT        NOT NULL COMMENT 'Trimestre (1 a 4)',
    semestre                TINYINT        NOT NULL COMMENT 'Semestre (1 ou 2)',
    dia_semana              VARCHAR(20)    NOT NULL COMMENT 'Nome do dia da semana em inglês maiúsculo (ex: MONDAY)',
    indicador_fim_semana    CHAR(3)        NOT NULL COMMENT 'SIM = sábado ou domingo | NAO = dia útil',
    PRIMARY KEY (sk_tempo)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Dimensão temporal — granularidade: dia';


-- ==============================================================
-- DIMENSÃO 2 — Dim_Veiculo
-- ==============================================================
CREATE TABLE IF NOT EXISTS Dim_Veiculo (
    sk_veiculo                          INT            NOT NULL COMMENT 'Surrogate Key do veículo',
    ano_fabricacao                      SMALLINT       NOT NULL COMMENT 'Ano de fabricação do veículo',
    marca                               VARCHAR(100)   NOT NULL COMMENT 'Marca do veículo (ex: FORD, CHEVROLET)',
    modelo                              VARCHAR(100)   NOT NULL COMMENT 'Modelo do veículo (ex: F-150, CAMARO)',
    versao                              VARCHAR(150)            COMMENT 'Versão / Trim (ex: SE, SPORT, LTZ)',
    tipo_carroceria                     VARCHAR(100)            COMMENT 'Tipo de carroceria (ex: SEDAN, SUV, PICKUP)',
    chassi                              VARCHAR(50)             COMMENT 'Número do chassi / VIN',
    idade_veiculo_no_momento_da_venda   TINYINT                 COMMENT 'Idade do veículo em anos no momento da venda',
    faixa_idade_veiculo                 VARCHAR(50)             COMMENT 'Faixa de odômetro (ex: 30.001 - 60.000 MI)',
    cor_interna                         VARCHAR(50)             COMMENT 'Cor do interior (ex: BLACK, TAN)',
    cor_externa                         VARCHAR(50)             COMMENT 'Cor externa (ex: WHITE, SILVER)',
    odometro                            DECIMAL(10,1)           COMMENT 'Quilometragem registrada em milhas',
    categoria                           VARCHAR(20)    NOT NULL COMMENT 'Categoria: NOVO | SEMINOVO | USADO',
    transmissao                         VARCHAR(20)    NOT NULL COMMENT 'AUTOMATICO | MANUAL | NAO INFORMADO',
    PRIMARY KEY (sk_veiculo)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Dimensão do veículo comercializado';


-- ==============================================================
-- DIMENSÃO 3 — Dim_Loja_Venda
-- ==============================================================
CREATE TABLE IF NOT EXISTS Dim_Loja_Venda (
    sk_loja             INT            NOT NULL COMMENT 'Surrogate Key da loja/revendedor',
    nome_loja           VARCHAR(250)   NOT NULL COMMENT 'Nome normalizado da loja/revendedor',
    estado_loja         CHAR(2)        NOT NULL COMMENT 'Sigla do estado americano (ex: CA, TX)',
    nome_estado_loja    VARCHAR(100)   NOT NULL COMMENT 'Nome completo do estado (ex: CALIFORNIA)',
    regiao_loja         VARCHAR(50)    NOT NULL COMMENT 'Região dos EUA (NORDESTE | SUL | CENTRO-OESTE | OESTE)',
    PRIMARY KEY (sk_loja)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Dimensão da loja/revendedor que realizou a venda';


-- ==============================================================
-- TABELA FATO — Fato_Vendas_Carros
-- ==============================================================
CREATE TABLE IF NOT EXISTS Fato_Vendas_Carros (
    id_fato             BIGINT         NOT NULL AUTO_INCREMENT COMMENT 'Chave primária técnica (surrogate)',
    sk_tempo_venda      INT            NOT NULL COMMENT 'FK → Dim_Tempo_Venda',
    sk_veiculo          INT            NOT NULL COMMENT 'FK → Dim_Veiculo',
    sk_loja             INT            NOT NULL COMMENT 'FK → Dim_Loja_Venda',
    quantidade_vendida  TINYINT        NOT NULL DEFAULT 1   COMMENT 'Sempre 1 — granularidade: 1 venda por registro',
    preco_venda         DECIMAL(12,2)  NOT NULL COMMENT 'Preço efetivamente praticado na venda (USD)',
    preco_mercado       DECIMAL(12,2)  NOT NULL COMMENT 'Preço de mercado Manheim (MMR) na época da venda (USD)',
    PRIMARY KEY (id_fato),
    CONSTRAINT fk_fato_tempo   FOREIGN KEY (sk_tempo_venda) REFERENCES Dim_Tempo_Venda(sk_tempo),
    CONSTRAINT fk_fato_veiculo FOREIGN KEY (sk_veiculo)     REFERENCES Dim_Veiculo(sk_veiculo),
    CONSTRAINT fk_fato_loja    FOREIGN KEY (sk_loja)        REFERENCES Dim_Loja_Venda(sk_loja)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
  COMMENT='Tabela Fato — 1 linha = 1 venda de veículo realizada';


-- ==============================================================
-- ÍNDICES — performance em queries analíticas
-- ==============================================================
CREATE INDEX idx_fato_tempo   ON Fato_Vendas_Carros(sk_tempo_venda);
CREATE INDEX idx_fato_veiculo ON Fato_Vendas_Carros(sk_veiculo);
CREATE INDEX idx_fato_loja    ON Fato_Vendas_Carros(sk_loja);
CREATE INDEX idx_veiculo_marca_modelo ON Dim_Veiculo(marca, modelo);
CREATE INDEX idx_tempo_ano_mes        ON Dim_Tempo_Venda(ano, mes);
CREATE INDEX idx_loja_estado          ON Dim_Loja_Venda(estado_loja);


-- ==============================================================
-- DADOS AMOSTRAIS — para validação do modelo
-- ==============================================================

-- Dim_Tempo_Venda (5 amostras)
INSERT INTO Dim_Tempo_Venda
  (sk_tempo, data_completa, ano, mes, nome_mes, numero_mes, dia,
   trimestre, semestre, dia_semana, indicador_fim_semana)
VALUES
  (1, '2014-12-16', 2014, 12, 'DECEMBER',  12, 16, 4, 2, 'TUESDAY',   'NAO'),
  (2, '2014-01-05', 2014,  1, 'JANUARY',    1,  5, 1, 1, 'SUNDAY',    'SIM'),
  (3, '2013-06-22', 2013,  6, 'JUNE',       6, 22, 2, 1, 'SATURDAY',  'SIM'),
  (4, '2012-03-10', 2012,  3, 'MARCH',      3, 10, 1, 1, 'SATURDAY',  'SIM'),
  (5, '2010-09-15', 2010,  9, 'SEPTEMBER',  9, 15, 3, 2, 'WEDNESDAY', 'NAO');

-- Dim_Veiculo (5 amostras)
INSERT INTO Dim_Veiculo
  (sk_veiculo, ano_fabricacao, marca, modelo, versao, tipo_carroceria,
   chassi, idade_veiculo_no_momento_da_venda, faixa_idade_veiculo,
   cor_interna, cor_externa, odometro, categoria, transmissao)
VALUES
  (1, 2014, 'FORD',       'F-150',   'XLT',     'PICKUP',  '1FTFX1CT0EKE00001', 0,  '0 - 15.000 MI',         'BLACK', 'WHITE',  4500.0,  'NOVO',     'AUTOMATICO'),
  (2, 2012, 'CHEVROLET',  'CAMARO',  'SS',       'COUPE',   '2G1FK1EJ5C9100002', 2,  '30.001 - 60.000 MI',    'BLACK', 'RED',   45200.0,  'SEMINOVO', 'MANUAL'),
  (3, 2010, 'TOYOTA',     'CAMRY',   'LE',       'SEDAN',   '4T1BF3EK2AU100003', 4,  '60.001 - 100.000 MI',   'TAN',   'SILVER', 78000.0, 'USADO',    'AUTOMATICO'),
  (4, 2008, 'HONDA',      'CIVIC',   'LX',       'SEDAN',   '1HGCP2F35BA100004', 6,  'ACIMA DE 100.000 MI',   'GRAY',  'BLACK', 112000.0, 'USADO',    'AUTOMATICO'),
  (5, 2013, 'BMW',        '3 SERIES','328I',     'SEDAN',   'WBA3A5C57DF100005', 1,  '15.001 - 30.000 MI',    'BLACK', 'BLUE',  22000.0,  'SEMINOVO', 'AUTOMATICO');

-- Dim_Loja_Venda (5 amostras)
INSERT INTO Dim_Loja_Venda
  (sk_loja, nome_loja, estado_loja, nome_estado_loja, regiao_loja)
VALUES
  (1, 'ADESA CALIFORNIA',      'CA', 'CALIFORNIA',    'OESTE'),
  (2, 'MANHEIM TEXAS',         'TX', 'TEXAS',         'SUL'),
  (3, 'ADESA FLORIDA',         'FL', 'FLORIDA',       'SUL'),
  (4, 'MANHEIM NEW YORK',      'NY', 'NEW YORK',      'NORDESTE'),
  (5, 'ADESA ILLINOIS',        'IL', 'ILLINOIS',      'CENTRO-OESTE');

-- Fato_Vendas_Carros (5 amostras)
INSERT INTO Fato_Vendas_Carros
  (sk_tempo_venda, sk_veiculo, sk_loja, quantidade_vendida, preco_venda, preco_mercado)
VALUES
  (1, 1, 1, 1, 28500.00, 29000.00),
  (2, 2, 2, 1, 32000.00, 33500.00),
  (3, 3, 3, 1, 14200.00, 14800.00),
  (4, 4, 4, 1,  9500.00, 10200.00),
  (5, 5, 5, 1, 31000.00, 32500.00);


-- ==============================================================
-- VIEWS ANALÍTICAS (bônus)
-- ==============================================================

-- View: resumo de vendas por marca e ano
CREATE OR REPLACE VIEW vw_vendas_por_marca_ano AS
SELECT
    t.ano,
    v.marca,
    COUNT(*)               AS total_vendas,
    ROUND(AVG(f.preco_venda), 2)    AS preco_medio_venda,
    ROUND(AVG(f.preco_mercado), 2)  AS preco_medio_mercado,
    ROUND(SUM(f.preco_venda), 2)    AS receita_total
FROM Fato_Vendas_Carros f
JOIN Dim_Tempo_Venda  t ON f.sk_tempo_venda = t.sk_tempo
JOIN Dim_Veiculo      v ON f.sk_veiculo     = v.sk_veiculo
GROUP BY t.ano, v.marca
ORDER BY t.ano, receita_total DESC;

-- View: desempenho por loja
CREATE OR REPLACE VIEW vw_desempenho_por_loja AS
SELECT
    l.nome_loja,
    l.estado_loja,
    l.regiao_loja,
    COUNT(*)                        AS total_vendas,
    ROUND(AVG(f.preco_venda), 2)    AS ticket_medio,
    ROUND(SUM(f.preco_venda), 2)    AS receita_total
FROM Fato_Vendas_Carros f
JOIN Dim_Loja_Venda l ON f.sk_loja = l.sk_loja
GROUP BY l.nome_loja, l.estado_loja, l.regiao_loja
ORDER BY receita_total DESC;

-- View: diferença preço venda vs mercado
CREATE OR REPLACE VIEW vw_diferenca_preco AS
SELECT
    v.marca,
    v.modelo,
    t.ano,
    ROUND(AVG(f.preco_venda), 2)                                AS avg_preco_venda,
    ROUND(AVG(f.preco_mercado), 2)                              AS avg_preco_mercado,
    ROUND(AVG(f.preco_venda - f.preco_mercado), 2)              AS diferenca_media,
    ROUND(AVG((f.preco_venda/f.preco_mercado - 1) * 100), 2)    AS variacao_pct
FROM Fato_Vendas_Carros f
JOIN Dim_Veiculo     v ON f.sk_veiculo     = v.sk_veiculo
JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
GROUP BY v.marca, v.modelo, t.ano
ORDER BY variacao_pct DESC;
