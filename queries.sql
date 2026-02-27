-- Desafio 1: Ranking de Liberação de Crédito por Agência
-- A diretoria quer premiar as agências com melhor desempenho. Crie uma query que retorne os top 2 maiores empréstimos
-- (em Valor_Contratado) de cada agência no ano de 2024. O resultado deve mostrar o nome da agência, o nome do cooperado, 
-- o valor do empréstimo e a posição dele no ranking interno daquela agência. Caso haja empate no valor, eles devem ocupar a mesma posição.

with rank_agencias as (
select a.Nome_Agencia, co.nome_completo, e.Valor_Contratado,
dense_rank() over (PARTITION BY a.id_agencia ORDER BY e.Valor_Contratado desc) as posicao
from agencia a
join cooperado co on a.ID_Agencia = co.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
where e.Data_Contratacao >= '2024-01-01' and e.Data_Contratacao < '2025-01-01'
)

select * from rank_agencias
where posicao <=2;


-- Desafio 2: Análise de Ticket Médio e Variação (Benchmarking)
-- Para avaliar se estamos concedendo muito ou pouco crédito em contratos individuais,
-- liste todos os empréstimos Ativos. Para cada um, traga o nome do cooperado, a modalidade de crédito, o valor contratado,
-- a média histórica de valor contratado para aquela modalidade específica (usando Window Functions) 
-- e uma coluna calculada mostrando a diferença percentual entre o contrato atual e a média da modalidade.

with emprestimos as (
select co.Nome_Completo, md.Nome_Modalidade, e.Valor_Contratado as contrato_atual, e.Status_Contrato,
round(avg(e.Valor_Contratado) over (PARTITION BY md.Nome_Modalidade),2) as média_histórica
from cooperado co
join conta c on co.id_cooperado = c.ID_Cooperado
join emprestimo e on c.ID_conta = e.ID_conta
join Modalidade_Credito md on e.ID_Modalidade = md.ID_Modalidade
)

select Nome_Completo, Nome_Modalidade, contrato_atual,
round(((contrato_atual / média_histórica)-1) * 100, 2) as diferença_percentual
from emprestimos
where Status_Contrato = 'Ativo';


-- Desafio 3: Mapa da Inadimplência por Perfil de Produtor
-- O setor de risco precisa entender onde está o gargalo da inadimplência. Crie um relatório sumarizado que exija uma CTE.
-- O relatório deve mostrar: O Perfil_Produtor, o volume total (soma) de crédito contratado na história, 
-- o volume total de crédito atrelado a contratos com status Inadimplente, 
-- e a Taxa de Inadimplência Financeira (Valor Inadimplente / Valor Total * 100).

with Mapa_Inadimplência as (
select co.Perfil_Produtor, 
sum(e.Valor_Contratado) as volume_total,
sum(
    case when e.Status_Contrato = 'Inadimplente' then e.Valor_Contratado
    else 0
    end
) as total_inadimplente
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
group by co.Perfil_Produtor
)

select *,
round((total_inadimplente / NULLIF(volume_total,0)) * 100.0, 2) as Taxa_Inadimplência_Financeira
from Mapa_Inadimplência;


-- Desafio 4: Identificação de Outliers no Risco
-- Precisamos identificar cooperados que pegaram empréstimos muito acima do padrão do seu perfil,
-- pois isso representa um risco atípico. Liste todos os empréstimos cujo Valor_Contratado seja superior
-- ao dobro da média dos empréstimos concedidos para aquele mesmo Perfil_Produtor.

with emprestimos_com_media as (
select co.Nome_Completo, co.Perfil_Produtor,
e.Valor_Contratado,
avg(e.Valor_Contratado) over (PARTITION BY co.Perfil_Produtor) as media_perfil
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
)

select * from emprestimos_com_media
where Valor_Contratado > media_perfil * 2;


-- Desafio 5: Curva de Crescimento Mensal (MoM - Month over Month)
-- Para a reunião de planejamento estratégico, crie uma análise usando CTE e Window Functions que mostre a evolução da concessão de crédito no ano de 2023.
-- O resultado deve ter: Mês de contratação, Valor total contratado naquele mês, o Valor total contratado no mês imediatamente anterior,
-- e o percentual de crescimento ou queda em relação ao mês anterior.

WITH mes_total_contratado AS (
    SELECT 
        substr(e.Data_Contratacao, 1, 7) AS mes,
        SUM(e.Valor_Contratado) AS total_atual
    FROM Cooperado co
    JOIN Conta ct 
        ON co.ID_Cooperado = ct.ID_Cooperado
    JOIN Emprestimo e 
        ON ct.ID_Conta = e.ID_Conta
    WHERE substr(e.Data_Contratacao, 1, 4) = '2023'
    GROUP BY substr(e.Data_Contratacao, 1, 7)
),
mes_com_anterior AS (
    SELECT 
        mes,
        total_atual,
        LAG(total_atual) OVER (ORDER BY mes) AS total_mes_anterior
    FROM mes_total_contratado
)

SELECT 
    mes,
    total_atual,
    total_mes_anterior,
    ROUND(
        (total_atual - total_mes_anterior)
        / NULLIF(total_mes_anterior, 0)
        * 100.0
    , 2) AS crescimento_percentual
FROM mes_com_anterior
ORDER BY mes;


-- Desafio 6: Risco Oculto (Correlated Subquery)
-- O status do contrato pode estar como 'Ativo', mas o cliente pode já estar com parcelas atrasadas. 
-- Usando uma Subquery Correlacionada, liste o Nome do Cooperado, a Agência e o Valor Contratado de todos os contratos que estão com status Ativo,
-- mas que possuem pelo menos uma parcela com status Atrasada.

select co.Nome_Completo, a.Nome_Agencia, 
e.Valor_Contratado
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
where e.Status_Contrato = 'Ativo' AND EXISTS (
        SELECT 1
        FROM Parcela p
        WHERE 
            p.ID_Emprestimo = e.ID_Emprestimo
            AND p.Status_Parcela = 'Atrasada'
    )
group by co.Nome_Completo, a.Nome_Agencia, e.Valor_Contratado;


-- Desafio 7: Análise de Pareto por Agência (Concentração de Risco)
-- A diretoria quer ver o nível de concentração de dinheiro por agência. Para a "Sede Campo Mourão" (ID 1), 
-- liste todos os contratos Ativos em ordem decrescente de valor. Usando Window Functions de agregação, 
-- adicione uma coluna de Soma Acumulada (Running Total) do valor contratado.

with dinheiro_por_agencia as (
select e.ID_Emprestimo,
        co.Nome_Completo,
        a.Nome_Agencia,
        e.Valor_Contratado
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
where a.id_agencia = 1 and e.Status_Contrato = 'Ativo'
)

select *,
sum(Valor_Contratado) over (ORDER BY Valor_Contratado desc, ID_Emprestimo asc) as Running_Total
from dinheiro_por_agencia
ORDER BY Valor_Contratado DESC;


-- Desafio 8: Engajamento e Retenção de Bons Pagadores
-- O time de marketing quer criar uma campanha de "Crédito Pré-Aprovado" para os melhores clientes. 
-- Identifique os cooperados que se encaixam na seguinte regra: Já quitaram pelo menos um empréstimo na história (status Quitado) 
-- E possuem conta Ativa com saldo positivo. Retorne o nome do cooperado, cidade da agência e o saldo atual.

select co.Nome_Completo, a.Cidade, ct.Saldo_Atual
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
where ct.Status_Conta = 'Ativa' and ct.Saldo_Atual >= 0 and EXISTS (
    select 1
    from emprestimo e
    where e.Status_Contrato = 'Quitado'
    and e.ID_Conta = ct.ID_Conta
);


-- Desafio 9: Fluxo de Caixa Realizado vs Esperado (DRE Simplificado)
-- Para o fechamento trimestral, a contabilidade quer saber quanto dinheiro de fato entrou na cooperativa de parcelas pagas contra o que
-- ainda está na rua aguardando pagamento. Crie um relatório por Nome_Modalidade que exiba: O valor total de parcelas com status Paga e o 
-- valor total de parcelas com status Pendente ou Atrasada. Ordene pelas modalidades que mais geraram caixa (Pagas).

select 
mc.nome_modalidade,
sum(
    case when p.Status_Parcela = 'Paga' then Valor_Parcela
    else 0
    end
) as soma_status_pagas,
SUM(
        CASE 
            WHEN p.Status_Parcela IN ('Pendente','Atrasada')
            THEN p.Valor_Parcela 
            ELSE 0 
        END
    ) AS parcelas_atrasadas_pendentes
from parcela p
join emprestimo e on p.id_emprestimo = e.id_emprestimo
join Modalidade_Credito mc on e.ID_Modalidade = mc.ID_Modalidade
group by mc.nome_modalidade
order by soma_status_pagas desc;


-- Desafio 10: Score de Risco Dinâmico do Cliente
-- Crie um "Score de Risco" para cada cooperado baseado no seu histórico de parcelas usando CASE combinado com CTEs ou agrupamentos.
-- Regra:
-- 0 parcelas atrasadas no histórico: 'Risco Baixo'
-- 1 parcela atrasada no histórico: 'Risco Médio'
-- 2 ou mais parcelas atrasadas: 'Risco Alto'
-- O resultado final não deve listar os clientes, mas sim um resumo gerencial: Agência | Classificação de Risco | Quantidade de Cooperados

with atrasos_por_cooperado as (
select 
a.Nome_Agencia as Agência,
sum(
    case when p.Status_Parcela = 'Atrasada' then 1 
    else 0
    end
) as total_atrasos
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
join parcela p on e.id_emprestimo = p.ID_Emprestimo
group by a.Nome_Agencia, co.ID_Cooperado
)

select Agência,
case 
        WHEN total_atrasos = 0 THEN 'Risco Baixo'
        WHEN total_atrasos = 1 THEN 'Risco Médio'
        ELSE 'Risco Alto'
end as Classificação_de_Risco,
COUNT(*) AS Quantidade_Cooperados
from atrasos_por_cooperado
GROUP BY 
    Agência,
    CASE 
        WHEN total_atrasos = 0 THEN 'Risco Baixo'
        WHEN total_atrasos = 1 THEN 'Risco Médio'
        ELSE 'Risco Alto'
    END
ORDER BY Agência;


-- Desafio 11: Clientes "Fantasmas" (Limpeza de Base)
-- O time de CRM percebeu que temos custos de manutenção com contas que não geram receita.
-- A Tarefa: Encontre todos os cooperados que possuem uma Conta (seja Ativa, Inativa ou Bloqueada), 
-- mas que nunca contrataram nenhum empréstimo na história da cooperativa. Retorne o nome do cooperado, 
-- a cidade da agência e a data de associação.

select co.ID_Cooperado, co.Nome_Completo, a.Cidade, co.Data_Associacao
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
left join emprestimo e on ct.ID_Conta = e.ID_Conta
where ct.Status_Conta in ('Ativa', 'Inativa', 'Bloqueada')
AND NOT EXISTS (
    SELECT 1
    FROM conta ct2
    JOIN emprestimo e 
        ON ct2.ID_Conta = e.ID_Conta
    WHERE ct2.ID_Cooperado = co.ID_Cooperado
);


-- Desafio 12: Conciliação de Saldo vs. Dívida (Auditoria de Risco)
-- A auditoria quer encontrar clientes que estão à beira da falência dentro do nosso ecossistema.
-- A Tarefa: Liste os cooperados cujo saldo atual da conta corrente é menor do que a soma de todas as suas parcelas não pagas (Pendente + Atrasada). 
-- Retorne o nome do cliente, o saldo atual da conta e o valor total da dívida pendente.

with parcelas_nao_pagas as (
select co.ID_Cooperado, 
co.Nome_Completo,
sum(Valor_Parcela) as valor_total_dívidas_pendentes,
sum(ct.Saldo_Atual) as Saldo_Atual
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo
where p.Status_Parcela in ('Pendente', 'Atrasada')
group by co.ID_Cooperado, co.Nome_Completo
)

select Nome_Completo,
Saldo_Atual,
valor_total_dívidas_pendentes
from parcelas_nao_pagas
where Saldo_Atual < valor_total_dívidas_pendentes;


-- Desafio 13: Tempo Médio de Pagamento por Perfil (Análise de Comportamento)
-- O time de Risco quer saber se os Grandes Produtores pagam mais rápido que os Pequenos.
-- A Tarefa: Considerando apenas as parcelas com status Paga, calcule a diferença em dias entre a Data_Vencimento e a Data_Pagamento.
-- Depois, retorne o Perfil_Produtor e a média de dias de atraso/adiantamento para cada perfil. Valores negativos significam que pagaram adiantado.

select 
co.Perfil_Produtor,
round(avg(julianday(p.Data_Pagamento ) - julianday(p.Data_Vencimento)), 2) as media
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo
where p.Status_Parcela = 'Paga'
group by co.Perfil_Produtor
order by media desc;


-- Desafio 14: Segmentação de Clientes em Quartis (Curva ABC / NTILE)
-- Marketing quer lançar 4 campanhas diferentes baseadas no volume financeiro histórico dos clientes.
-- A Tarefa: Usando a função analítica NTILE(4), classifique todos os cooperados que já pegaram algum empréstimo em 4 "Tiers" (1, 2, 3 e 4) 
-- baseados na soma total do Valor_Contratado na história. O Tier 1 deve conter os maiores tomadores de crédito. Retorne o nome, 
-- o volume total e o número do Tier.

with total_por_cooperado as (
select 
co.id_cooperado,
co.Nome_Completo,
sum(e.Valor_Contratado) total
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
group by co.id_cooperado, co.Nome_Completo
)

select *,
NTILE(4) over (order by total desc) as tier 
from total_por_cooperado;


-- Desafio 15: O Próximo Vencimento Crítico (Ação Operacional)
-- Os gerentes das agências precisam ligar amanhã para os clientes lembrando-os da próxima parcela.
-- A Tarefa: Para cada contrato com status Ativo, descubra qual é o valor e a data da próxima parcela a vencer 
-- (ou seja, a parcela com status Pendente que tem a menor Data_Vencimento).
-- Retorne o ID do Empréstimo, o Nome do Cooperado, o Valor dessa parcela específica e a Data de Vencimento dela.

with data_proxima_parcela as (
select 
e.ID_Emprestimo,
co.Nome_Completo,
p.Valor_Parcela,
p.Data_Vencimento,
row_number() over (PARTITION BY e.ID_Emprestimo order by p.Data_Vencimento) as proxima_parcela
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
join parcela p on e.id_emprestimo = p.ID_Emprestimo
where e.Status_Contrato = 'Ativo' and p.Status_Parcela = 'Pendente'
)

select ID_Emprestimo,
Nome_Completo,
Valor_Parcela,
Data_Vencimento
from data_proxima_parcela
where proxima_parcela = 1
order by proxima_parcela asc;


-- Desafio 16: Taxa de Juros Média Ponderada por Agência
-- O Banco Central exige saber o custo médio do dinheiro emprestado por região.
-- A Tarefa: Calcule a Taxa de Juros Efetiva Ponderada para os empréstimos Ativos de cada Agência. 
-- A fórmula da média ponderada é: Soma(Valor_Contratado * Taxa_Juros_Anual) / Soma(Valor_Contratado).
-- Mostre o nome da agência e a taxa média ponderada formatada com duas casas decimais.

select a.Nome_Agencia,
round(sum(e.Valor_Contratado * md.Taxa_Juros_Anual) / nullif(sum(e.Valor_Contratado),0),2) as taxa_média_ponderada
from cooperado co
join agencia a on co.ID_Agencia  = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
join modalidade_credito md on e.ID_Modalidade = md.ID_Modalidade
where e.Status_Contrato = 'Ativo'
group by a.ID_Agencia, a.Nome_Agencia
order by taxa_média_ponderada desc;


-- Desafio 17: O "Paradoxo do Prazo" (Análise de Desvio de Produto)
-- O time de Produtos criou modalidades com "Prazo Máximo de Meses", mas suspeita que os gerentes estão quebrando a regra na hora de gerar as parcelas.
-- A Tarefa: Liste o Nome_Modalidade e o Prazo_Maximo_Meses. Ao lado, exiba a quantidade máxima de parcelas que algum contrato daquela modalidade já teve na prática.
-- Filtre (usando HAVING) para mostrar apenas as modalidades onde a quantidade de parcelas geradas no banco ultrapassou o prazo máximo permitido pela regra de negócio.

select md.Nome_Modalidade,
md.Prazo_Maximo_Meses,
max(p.Numero_Parcela) as maximo_parcelas_praticado
from Modalidade_Credito md 
join emprestimo e on md.ID_Modalidade = e.ID_Modalidade
join parcela p on e.id_emprestimo = p.ID_Emprestimo
group by md.Nome_Modalidade, md.Prazo_Maximo_Meses
having max(p.Numero_Parcela) > md.Prazo_Maximo_Meses;


-- Desafio 18: Suavização de Picos com Média Móvel (Rolling Average)
-- A diretoria está assustada com a variação brusca de valores contratados de um empréstimo para o outro e quer ver uma tendência mais suave na "Antecipação de Safra" (ID 4).
-- A Tarefa: Liste todos os empréstimos da modalidade 4 em ordem cronológica de contratação. Retorne a data, o valor contratado,
-- e crie uma coluna de Média Móvel dos últimos 3 empréstimos (o atual e os 2 imediatamente anteriores).

select e.Data_Contratacao,
e.Valor_Contratado,
avg(e.Valor_Contratado) over (order by e.Data_Contratacao, e.ID_Emprestimo ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as Média_Móvel
from emprestimo e
where e.ID_Modalidade = 4
order by e.Data_Contratacao;


-- Desafio 19: O "Caloteiro VIP" (Filtros Complexos Múltiplos)
-- Precisamos agir rápido sobre clientes de alta renda que estão dando prejuízo.
-- A Tarefa: Identifique os cooperados que se encaixam simultaneamente em três regras:
-- Possuem um volume histórico total contratado maior que R$ 100.000,00.
-- O Saldo_Atual da conta é negativo (menor que zero).
-- Possuem pelo menos 1 parcela com status Atrasada neste exato momento.
-- Retorne apenas o Nome do Cooperado, Telefone (não temos, então retorne a Cidade da Agência) e o Saldo Atual.

select co.Nome_Completo, a.Cidade, ct.Saldo_Atual
from cooperado co
join agencia a on co.ID_Agencia  = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
where ct.Saldo_Atual < 0 
and exists (
    select 1 
    from emprestimo e2
    join parcela p on e2.ID_Emprestimo = p.ID_Emprestimo
    where e2.ID_Conta = ct.ID_Conta 
    and p.Status_Parcela = 'Atrasada'
)
group by co.ID_Cooperado, co.Nome_Completo, a.Cidade, ct.Saldo_Atual
having sum(e.Valor_Contratado) > 100000;


-- Desafio 20: Faturamento vs. Inadimplência no Mesmo Relatório (PIVOT Simulado)
-- A diretoria quer um resumão na tela, uma linha por Agência, com três colunas de valores totais de Parcelas:
-- Nome da Agência.
-- Total em R$ de parcelas Pagas.
-- Total em R$ de parcelas Pendentes.
-- Total em R$ de parcelas Atrasadas.
-- A Tarefa: Crie essa query. 

select a.Nome_Agencia,
sum(case when p.Status_Parcela = 'Paga' then p.Valor_Parcela else 0 end) as total_parcelas_pagas,
sum(case when p.Status_Parcela = 'Pendente' then p.Valor_Parcela else 0 end) as total_parcelas_pendentes, 
sum(case when p.Status_Parcela = 'Atrasada' then p.Valor_Parcela else 0 end) as total_parcelas_atrasadas
from cooperado co
join agencia a on co.ID_Agencia  = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo 

group by a.Nome_Agencia;

-- Desafio 21: Fraude de "Associação Relâmpago" (Time-to-Action)
-- O time de Prevenção a Fraudes notou um padrão suspeito: pessoas se associando à cooperativa e pegando empréstimos gigantes logo em seguida, 
-- com intenção de calote.
-- A Tarefa: Liste todos os cooperados que contrataram seu primeiro empréstimo em um prazo de 30 dias ou menos após a sua Data_Associacao. 
-- Retorne o nome do cooperado, a data de associação, a data do empréstimo e a quantidade de dias entre os dois eventos.

with emprestimo_data as (
select co.ID_Cooperado, co.Nome_Completo, co.Data_Associacao, e.Data_Contratacao,
row_number() over (PARTITION BY co.ID_Cooperado order by e.Data_Contratacao asc) as primeiro_emprestimo_co
from cooperado co
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
),

diferenca_de_dias as (
select Nome_Completo, Data_Associacao, Data_Contratacao,
julianday(Data_Contratacao) - julianday(Data_Associacao) as diferenca_dias
from emprestimo_data
where primeiro_emprestimo_co = 1
)

select * from diferenca_de_dias
where diferenca_dias >= 0 and diferenca_dias <= 30;


-- Desafio 22: Auditoria de Sistema (Mismatch de Contrato vs. Parcelas)
-- A TI relatou um possível bug onde o sistema gerou parcelas erradas na hora de fechar o contrato. O valor total das parcelas deveria ser,
-- no mínimo, igual ou maior (devido aos juros) que o valor contratado.
-- A Tarefa: Encontre os contratos onde a soma de todas as parcelas geradas para ele seja estritamente menor que o Valor_Contratado do empréstimo.
-- Retorne o ID do Empréstimo, o Nome do Cliente, o Valor Contratado e a Soma das Parcelas.

select e.ID_Emprestimo, co.nome_completo, e.Valor_Contratado, 
COALESCE(sum(p.Valor_Parcela), 0) as total_parcelas
from cooperado co   
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
left join parcela p on e.ID_Emprestimo = p.ID_Emprestimo 
group by e.ID_Emprestimo, co.nome_completo, e.Valor_Contratado
having COALESCE(sum(p.Valor_Parcela), 0) < e.Valor_Contratado;


-- Desafio 23: Sazonalidade por Agência (Rank Temporal)
-- A diretoria quer saber qual foi o "Mês de Ouro" de cada agência no ano de 2024 para planejar as férias dos gerentes.
-- A Tarefa: Para cada Agência, descubra qual foi o Mês (ex: '05' para maio) que teve o maior volume total financeiro (Valor_Contratado)
-- liberado em empréstimos no ano de 2024. Retorne o Nome da Agência, o Mês de Ouro e o Valor Total Liberado naquele mês.

with total_e_mes as (
select a.ID_Agencia, a.Nome_Agencia, substr(e.Data_Contratacao, 1, 7) as mes, 
sum(e.Valor_Contratado) as total
from cooperado co   
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
where e.Data_Contratacao >= '2024-01-01'
and e.Data_Contratacao < '2025-01-01' 
group by a.ID_Agencia, a.Nome_Agencia, substr(e.Data_Contratacao, 1, 7)
),

rank as (
select Nome_Agencia, mes, total,
row_number() over (PARTITION BY Nome_Agencia order by total desc) as ranqueado
from total_e_mes
)

select Nome_Agencia, mes, total from rank
where ranqueado = 1;


-- Desafio 24: Tempo até a Ruína (Time-to-Default)
-- Para modelagem de risco, os cientistas de dados precisam saber quanto tempo um cliente "ruim" demora para dar o primeiro calote.
-- A Tarefa: Olhando apenas para os empréstimos que hoje estão com o status Inadimplente, calcule a diferença em dias entre a Data_Contratacao do empréstimo e a
-- Data_Vencimento da primeira parcela (a mais antiga) que ficou com status Atrasada. Retorne o ID do Empréstimo e os dias até a ruína.

select 
    e.ID_Emprestimo,
    min(julianday(p.Data_Vencimento)) - julianday(e.Data_Contratacao) as dias_ate_ruina
from emprestimo e
join parcela p 
    on e.ID_Emprestimo = p.ID_Emprestimo
where e.Status_Contrato = 'Inadimplente'
and p.Status_Parcela = 'Atrasada'
group by 
    e.ID_Emprestimo,
    e.Data_Contratacao;


-- Desafio 25: Clientes Recuperados (Análise de Cohort/Comportamento)
-- O time de CRM quer premiar clientes que já deram problema no passado, mas "limparam o nome" e voltaram a fazer negócio de forma saudável.
-- A Tarefa: Liste o nome dos cooperados que se encaixam na seguinte regra simultânea: Possuem pelo menos um contrato histórico com status Inadimplente
-- E possuem atualmente pelo menos um contrato com status Ativo.

select 
    co.Nome_Completo
from cooperado co
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.ID_Conta
group by co.ID_Cooperado, co.Nome_Completo
having
count(case when e.Status_Contrato = 'Inadimplente' then 1 end) > 0
and count(case when e.Status_Contrato = 'Ativo' then 1 end) > 0;


-- Desafio 26: O Efeito Robin Hood (Análise de Decis - NTILE 10)
-- O Risco quer validar uma tese: "Os maiores produtores são mais pontuais que os pequenos?". Vamos testar os extremos.
-- A Tarefa: Usando NTILE(10) baseado no volume financeiro total (Valor_Contratado) histórico de cada cliente, separe a base em 10 grupos.
-- Retorne apenas os clientes que caíram no Grupo 1 (os top 10% maiores tomadores) e os clientes que caíram no Grupo 10 (os bottom 10% menores tomadores).
-- Exiba o Nome, o Perfil do Produtor, o Volume Total e a qual Decil (1 ou 10) ele pertence.

with total_cooperados as (
select co.Nome_Completo, co.Perfil_Produtor,
sum(e.Valor_Contratado) as total
from cooperado co   
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
group by co.ID_Cooperado, co.Nome_Completo, co.Perfil_Produtor
),

classificacao  as (
select *,
ntile(10) over (order by total desc) as Decil
from total_cooperados
)

select * from classificacao  
where Decil in (1,10);


-- Desafio 27: A Linha do Tempo do Caixa (Running Total Cooperativo)
-- A tesouraria precisa ver o "sangramento" do caixa dia após dia no ano de 2024, independentemente da agência.
-- A Tarefa: Liste todas as datas exatas (dia, mês e ano) em que houve contratação de empréstimos em 2024. 
-- Mostre o valor total contratado naquele dia e adicione uma coluna de Soma Acumulada exibindo o montante total de dinheiro que já havia saído 
-- do caixa da AgroCred desde o dia 01/01/2024 até aquela data específica.

with total_por_dia as (
select sum(e.Valor_Contratado) as total, e.Data_Contratacao
from cooperado co   
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
where e.Data_Contratacao >= '2024-01-01' and e.Data_Contratacao < '2025-01-01'
group by e.Data_Contratacao
)

select *,
sum(total) over (order by Data_Contratacao) as soma_acumulada
from total_por_dia;


-- Desafio 28: Esforço de Cobrança (Proporção de Quantidade)
-- As agências estão reclamando que passam o dia todo ligando para cobrar clientes. Vamos descobrir quem trabalha mais nisso.
-- A Tarefa: Crie um relatório por Nome_Agencia que calcule a taxa de esforço de cobrança. A regra é: Quantidade de parcelas Atrasadas da agência dividida 
-- pela Quantidade Total de parcelas geradas por aquela agência (independente do status). Retorne a taxa em formato percentual com duas casas decimais. 
-- Aqui o foco é em contagem de boletos (COUNT), não em valor financeiro.

select a.Nome_Agencia,
round(
    count(case when p.Status_Parcela = 'Atrasada' then 1 end) * 100.0
    / nullif(count(p.id_parcela), 0),
2) as taxa_esforco_cobranca
from cooperado co   
join agencia a on co.ID_Agencia = a.ID_Agencia 
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo
group by a.ID_Agencia, a.Nome_Agencia;


-- Desafio 29: O Risco Especulativo (Cruzamento de Regras)
-- O Banco Central mandou um alerta sobre empréstimos de longo prazo para pequenos produtores.
-- A Tarefa: Encontre os contratos Ativos atrelados a clientes com Perfil_Produtor = 'Pequeno', onde o prazo gerado (quantidade de parcelas daquele empréstimo) 
-- seja maior que 24 meses, e onde a Taxa_Juros_Anual da modalidade escolhida seja superior a 10%. Retorne o nome do cliente,
-- a modalidade e a quantidade exata de parcelas daquele contrato.

select co.Nome_Completo, md.Nome_Modalidade, count(p.id_parcela) as total_parcelas
from cooperado co   
join conta ct on co.ID_Cooperado = ct.ID_Cooperado
join emprestimo e on ct.ID_Conta = e.id_conta
join Modalidade_Credito md on e.ID_Modalidade = md.ID_Modalidade
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo
where e.Status_Contrato = 'Ativo' and co.Perfil_Produtor = 'Pequeno'  and md.Taxa_Juros_Anual > 10
group by e.ID_Emprestimo, co.Nome_Completo, md.Nome_Modalidade
having count(p.id_parcela) > 24;


-- Desafio 30: Detecção de Duplicidade (Self-Join ou CTE avançada)
-- O sistema de pagamentos via PIX deu um "soluço" e suspeitamos que algumas parcelas foram registradas duas vezes para o mesmo contrato.
-- A Tarefa: Verifique se existe alguma anomalia na base: Liste os IDs de empréstimo que possuem mais de uma parcela cadastrada com exatamente o 
-- mesmo Numero_Parcela (por exemplo, duas parcelas "número 2" para o mesmo contrato).

select e.ID_Emprestimo 
from emprestimo e 
join parcela p on e.ID_Emprestimo = p.ID_Emprestimo
group by e.ID_Emprestimo, p.Numero_Parcela
having count(*) > 1;
