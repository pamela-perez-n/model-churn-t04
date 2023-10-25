-- Databricks notebook source
with tb_pedidos AS (

    SELECT t1.idPedido,
          t1.dtPedido,
          t1.dtAprovado,
          t1.dtEnvio,
          t1.dtEntregue,
          t1.dtEstimativaEntrega,
          t2.idVendedor,
          t2.idProduto,
          t2.vlPreco,
          t2.vlFrete,
          t3.descUF

    FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.cliente AS t3
    ON t1.idCliente = t3.idCliente

    WHERE t1.dtPedido < '2018-01-01'
    AND t2.idVendedor is not null
),

tb_agin AS (
    SELECT idVendedor,
          date_diff('2018-01-01',min(dtPedido)) AS nrIdade,
          date_diff('2018-01-01',max(dtPedido)) AS nrRecencia
    FROM tb_pedidos
    GROUP BY idVendedor
),

tb_pedidos_m12 AS (
    SELECT *
    FROM tb_pedidos
    WHERE dtPedido >= date('2018-01-01') - INTERVAL 1 YEAR -- 2017-01-01
),

tb_sumario_m12 AS (

    SELECT idVendedor,
          count(distinct idPedido) AS nrQtdePedido,
          count(idPedido) AS nrQtdeItensTotais,
          count(distinct idProduto) AS nrQtdeItensDistintos,
          sum(vlPreco) AS nrReceita,
          sum(vlPreco) / count(distinct idPedido) AS nrTicketMedio,
          sum(vlPreco) / count(idPedido) AS nrValorMedioItem,
          sum(vlFrete) / (sum(vlFrete+vlPreco)) AS pctFreteValor,
          count(idPedido) / count(distinct idPedido) AS nrQtdeItemPedido,
          count(distinct idPedido) / count(distinct date_trunc('MONTH', dtPedido)) AS nrMediaVendaMes,
          count(distinct date_trunc('MONTH', dtPedido)) / (int(date_diff('2018-01-01', min(dtPedido)) / 30) + 1) AS pctMesesAtivacao,
          count(distinct date(dtPedido)) AS nrDiasPedido,

          COUNT(DISTINCT CASE WHEN descuf = 'AC' THEN idPedido END) / count(distinct idPedido) AS pctPedidoAC,
          COUNT(DISTINCT CASE WHEN descuf = 'AL' THEN idPedido END) / count(distinct idPedido) AS pctPedidoAL,
          COUNT(DISTINCT CASE WHEN descuf = 'AM' THEN idPedido END) / count(distinct idPedido) AS pctPedidoAM,
          COUNT(DISTINCT CASE WHEN descuf = 'AP' THEN idPedido END) / count(distinct idPedido) AS pctPedidoAP,
          COUNT(DISTINCT CASE WHEN descuf = 'BA' THEN idPedido END) / count(distinct idPedido) AS pctPedidoBA,
          COUNT(DISTINCT CASE WHEN descuf = 'CE' THEN idPedido END) / count(distinct idPedido) AS pctPedidoCE,
          COUNT(DISTINCT CASE WHEN descuf = 'DF' THEN idPedido END) / count(distinct idPedido) AS pctPedidoDF,
          COUNT(DISTINCT CASE WHEN descuf = 'ES' THEN idPedido END) / count(distinct idPedido) AS pctPedidoES,
          COUNT(DISTINCT CASE WHEN descuf = 'GO' THEN idPedido END) / count(distinct idPedido) AS pctPedidoGO,
          COUNT(DISTINCT CASE WHEN descuf = 'MA' THEN idPedido END) / count(distinct idPedido) AS pctPedidoMA,
          COUNT(DISTINCT CASE WHEN descuf = 'MG' THEN idPedido END) / count(distinct idPedido) AS pctPedidoMG,
          COUNT(DISTINCT CASE WHEN descuf = 'MS' THEN idPedido END) / count(distinct idPedido) AS pctPedidoMS,
          COUNT(DISTINCT CASE WHEN descuf = 'MT' THEN idPedido END) / count(distinct idPedido) AS pctPedidoMT,
          COUNT(DISTINCT CASE WHEN descuf = 'PA' THEN idPedido END) / count(distinct idPedido) AS pctPedidoPA,
          COUNT(DISTINCT CASE WHEN descuf = 'PB' THEN idPedido END) / count(distinct idPedido) AS pctPedidoPB,
          COUNT(DISTINCT CASE WHEN descuf = 'PE' THEN idPedido END) / count(distinct idPedido) AS pctPedidoPE,
          COUNT(DISTINCT CASE WHEN descuf = 'PI' THEN idPedido END) / count(distinct idPedido) AS pctPedidoPI,
          COUNT(DISTINCT CASE WHEN descuf = 'PR' THEN idPedido END) / count(distinct idPedido) AS pctPedidoPR,
          COUNT(DISTINCT CASE WHEN descuf = 'RJ' THEN idPedido END) / count(distinct idPedido) AS pctPedidoRJ,
          COUNT(DISTINCT CASE WHEN descuf = 'RN' THEN idPedido END) / count(distinct idPedido) AS pctPedidoRN,
          COUNT(DISTINCT CASE WHEN descuf = 'RO' THEN idPedido END) / count(distinct idPedido) AS pctPedidoRO,
          COUNT(DISTINCT CASE WHEN descuf = 'RR' THEN idPedido END) / count(distinct idPedido) AS pctPedidoRR,
          COUNT(DISTINCT CASE WHEN descuf = 'RS' THEN idPedido END) / count(distinct idPedido) AS pctPedidoRS,
          COUNT(DISTINCT CASE WHEN descuf = 'SC' THEN idPedido END) / count(distinct idPedido) AS pctPedidoSC,
          COUNT(DISTINCT CASE WHEN descuf = 'SE' THEN idPedido END) / count(distinct idPedido) AS pctPedidoSE,
          COUNT(DISTINCT CASE WHEN descuf = 'SP' THEN idPedido END) / count(distinct idPedido) AS pctPedidoSP,
          COUNT(DISTINCT CASE WHEN descuf = 'TO' THEN idPedido END) / count(distinct idPedido) AS pctPedidoTO,

          avg(date_diff(dtEntregue, dtPedido)) AS nrTempoMedioEntrega,
          avg(date_diff(dtEntregue, dtEstimativaEntrega)) AS nrTempoMedioEntregaPromessa,
          count(distinct case when dtEntregue > dtEstimativaEntrega THEN idPedido END) AS nrQtdePedidosAtraso

    FROM tb_pedidos_m12

    group by idVendedor

),

tb_vendedor_mes AS (

    SELECT idVendedor,
          date(date_trunc('MONTH', dtPedido)) AS mesPedido,
          count(distinct idPedido) AS qtdePedido

    FROM tb_pedidos_m12

    GROUP BY idVendedor, mesPedido
    ORDER BY idVendedor, mesPedido
),

tb_sumario_mes AS (

    SELECT idVendedor,
          max(qtdePedido) AS nrMaxVendaMes,
          min(qtdePedido) AS nrMinVendaMes,
          median(qtdePedido) AS nrMedianVendaMes,
          coalesce(std(qtdePedido), 0) AS nrStdVendaMes

    FROM tb_vendedor_mes
    GROUP BY idVendedor

),

tb_venda_dias AS (

    select distinct idVendedor,
        date(dtPedido) As dtVenda
    from tb_pedidos_m12
    order by idVendedor

),

tb_lag_vendas AS (

    select *,
        lag(dtVenda) OVER (PARTITION BY idVendedor ORDER BY dtVenda) AS lagVenda
    from tb_venda_dias

),

tb_intervalo_venda AS (
    select idVendedor,
            max(date_diff(dtVenda, lagVenda)) AS nrMaxIntervaloVenda,
            min(date_diff(dtVenda, lagVenda)) AS nrMinIntervaloVenda,
            avg(date_diff(dtVenda, lagVenda)) AS nrAvgIntervaloVenda
    from tb_lag_vendas
    where lagVenda is not null
    group by idVendedor
),

tb_produto AS (

    SELECT idVendedor,
        idProduto,
        count(*) AS qtdeProduto,
        sum(vlPreco) AS vlPreco
    FROM tb_pedidos_m12
    group by idVendedor, idProduto
),

tb_total_produto AS (

    select idVendedor,
            sum(qtdeProduto) AS totalVendas,
            sum(vlPreco) AS totalvlPreco
    from tb_produto
    group by idVendedor
 
 ),

tb_produto_share (

    SELECT t1.idVendedor,
            max(t1.qtdeProduto / t2.totalVendas) AS nrMaxShareQtde,
            max(t1.vlPreco / t2.totalvlPreco) AS nrMaxShareValor
    FROM tb_produto AS t1
    
    LEFT JOIN tb_total_produto AS t2
    on t1.idVendedor = t2.idVendedor

    group by t1.idVendedor
    order by t1.idVendedor
 )

SELECT 

        t1.idVendedor,
        t1.nrIdade,
        t1.nrRecencia,

        t2.nrQtdePedido,
        t2.nrQtdeItensTotais,
        t2.nrQtdeItensDistintos,
        t2.nrReceita,
        t2.nrTicketMedio,
        t2.nrValorMedioItem,
        t2.pctFreteValor,
        t2.nrQtdeItemPedido,
        t2.nrMediaVendaMes,
        t2.pctMesesAtivacao,
        t2.nrDiasPedido,
        t2.pctPedidoAC,
        t2.pctPedidoAL,
        t2.pctPedidoAM,
        t2.pctPedidoAP,
        t2.pctPedidoBA,
        t2.pctPedidoCE,
        t2.pctPedidoDF,
        t2.pctPedidoES,
        t2.pctPedidoGO,
        t2.pctPedidoMA,
        t2.pctPedidoMG,
        t2.pctPedidoMS,
        t2.pctPedidoMT,
        t2.pctPedidoPA,
        t2.pctPedidoPB,
        t2.pctPedidoPE,
        t2.pctPedidoPI,
        t2.pctPedidoPR,
        t2.pctPedidoRJ,
        t2.pctPedidoRN,
        t2.pctPedidoRO,
        t2.pctPedidoRR,
        t2.pctPedidoRS,
        t2.pctPedidoSC,
        t2.pctPedidoSE,
        t2.pctPedidoSP,
        t2.pctPedidoTO,
        t2.nrTempoMedioEntrega,
        t2.nrTempoMedioEntregaPromessa,
        t2.nrQtdePedidosAtraso,
        t3.nrMaxVendaMes,
        t3.nrMinVendaMes,
        t3.nrMedianVendaMes,
        t3.nrStdVendaMes,
        t4.nrMaxIntervaloVenda,
        t4.nrMinIntervaloVenda,
        t4.nrAvgIntervaloVenda,
        t5.nrMaxShareQtde,
        t5.nrMaxShareValor
 
FROM tb_agin as t1

LEFT JOIN tb_sumario_m12 as t2
ON t1.idvendedor = t2.idVendedor

LEFT JOIN tb_sumario_mes as t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo_venda as t4
ON t1.idVendedor = t4.idVendedor

LEFT JOIN tb_produto_share as t5
ON t1.idVendedor = t5.idVendedor

WHERE t2.idVendedor IS NOT NULL
