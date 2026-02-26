# AgroCred SQL Analytics 

Repositório com soluções SQL para análise de dados e gestão de risco em uma cooperativa de crédito agrícola.

## 🚀 Tecnologias e Técnicas Utilizadas
- **Motor:** SQLite (compatível com PostgreSQL/SQL Server com ajustes mínimos).
- **Window Functions:** `RANK`, `DENSE_RANK`, `LAG`, `AVG() OVER`, `SUM() OVER`, `NTILE`.
- **Lógica de Negócio:** Common Table Expressions (CTEs), Subqueries Correlacionadas (`EXISTS`), e Filtros de Agregação (`HAVING`).
- **Data Wrangling:** Tratamento de nulos com `NULLIF`, manipulação de datas com `julianday` e PIVOT manual com `CASE WHEN`.

## 📊 Principais Desafios Resolvidos
1. **Risco e Compliance:** Identificação de "Caloteiros VIP" e outliers de crédito por perfil de produtor.
2. **Performance de Vendas:** Ranking das agências que mais liberaram crédito em 2024.
3. **Crescimento (Growth):** Cálculo de crescimento mês a mês (Month-over-Month) com percentual de variação.
4. **Financeiro:** Média móvel de contratações, taxa de juros ponderada e conciliação de saldo vs. dívida.
5. **CRM:** Localização de clientes inativos ("fantasmas") e segmentação por quartis de faturamento.

## 🛠️ Esquema de Criação (DDL)
```sql
CREATE TABLE Agencia (
    ID_Agencia INTEGER PRIMARY KEY,
    Nome_Agencia TEXT,
    Cidade TEXT
);

CREATE TABLE Cooperado (
    ID_Cooperado INTEGER PRIMARY KEY,
    Nome_Completo TEXT,
    Perfil_Produtor TEXT, -- 'Pequeno', 'Médio', 'Grande'
    Data_Associacao DATE,
    ID_Agencia INTEGER,
    FOREIGN KEY (ID_Agencia) REFERENCES Agencia(ID_Agencia)
);

CREATE TABLE Conta (
    ID_Conta INTEGER PRIMARY KEY,
    ID_Cooperado INTEGER,
    Saldo_Atual DECIMAL(15,2),
    Status_Conta TEXT, -- 'Ativa', 'Inativa', 'Bloqueada'
    FOREIGN KEY (ID_Cooperado) REFERENCES Cooperado(ID_Cooperado)
);

CREATE TABLE Modalidade_Credito (
    ID_Modalidade INTEGER PRIMARY KEY,
    Nome_Modalidade TEXT,
    Taxa_Juros_Anual DECIMAL(5,2),
    Prazo_Maximo_Meses INTEGER
);

CREATE TABLE Emprestimo (
    ID_Emprestimo INTEGER PRIMARY KEY,
    ID_Conta INTEGER,
    ID_Modalidade INTEGER,
    Valor_Contratado DECIMAL(15,2),
    Data_Contratacao DATE,
    Status_Contrato TEXT, -- 'Ativo', 'Quitado', 'Inadimplente'
    FOREIGN KEY (ID_Conta) REFERENCES Conta(ID_Conta),
    FOREIGN KEY (ID_Modalidade) REFERENCES Modalidade_Credito(ID_Modalidade)
);

CREATE TABLE Parcela (
    ID_Parcela INTEGER PRIMARY KEY,
    ID_Emprestimo INTEGER,
    Numero_Parcela INTEGER,
    Valor_Parcela DECIMAL(15,2),
    Data_Vencimento DATE,
    Data_Pagamento DATE,
    Status_Parcela TEXT, -- 'Pendente', 'Paga', 'Atrasada'
    FOREIGN KEY (ID_Emprestimo) REFERENCES Emprestimo(ID_Emprestimo)
);
---
