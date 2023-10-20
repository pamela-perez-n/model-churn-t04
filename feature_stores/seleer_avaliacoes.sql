-- Databricks notebook source
--  percentual de avaliações respondidas

WITH tb_pedidos_avaliacoes AS (

    SELECT t1.idPedido,
          t2.idVendedor,
          t3.idPedido AS idPedidoAvaliado,
          t3.vlNota,
          t3.dtAvaliacao,
          t3.dtResposta,
          CASE WHEN t3.vlNota <= 2 THEN 'baixa'
                WHEN t3.vlNota <= 3 THEN 'neutra'
                WHEN t3.vlNota <= 5 THEN 'alta'
          END AS descAvaliacao 

    FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.avaliacao_pedido AS t3
    ON t1.idPedido = t3.idPedido

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= '2017-01-01'
    AND t1.dtEntregue <  DATE('2018-01-01') - INTERVAL 7 DAYS
    -- Pedidos que tem mais de 7 dias que foram entregues

),

tb_group (

  SELECT 
          idVendedor,
          avg(vlNota) AS avgNotaAvaliacao,
          
          count(distinct idPedidoAvaliado) / count(distinct idPedido) AS pctPedidoAvaliados,

          SUM(CASE WHEN descAvaliacao = 'baixa' THEN 1 ELSE 0 END) / count( idPedido) AS descAvaliacaoBaixaPedido,
          SUM(CASE WHEN descAvaliacao = 'neutra' THEN 1 ELSE 0 END) / count( idPedido) AS descAvaliacaoNeutraPedido,
          SUM(CASE WHEN descAvaliacao = 'alta' THEN 1 ELSE 0 END) / count( idPedido) AS descAvaliacaoAltaPedido,

          SUM(CASE WHEN descAvaliacao = 'baixa' THEN 1 ELSE 0 END) / count( idPedidoAvaliado) AS descAvaliacaoBaixaPedidoAvaliado,
          SUM(CASE WHEN descAvaliacao = 'neutra' THEN 1 ELSE 0 END) / count( idPedidoAvaliado) AS descAvaliacaoNeutraPedidoAvaliado,
          SUM(CASE WHEN descAvaliacao = 'alta' THEN 1 ELSE 0 END) / count( idPedidoAvaliado) AS descAvaliacaoAltaPedidoAvaliado,

          avg(date_diff(dtResposta, dtAvaliacao)) AS avgTempoTespostaAvaliacao,
          median(date_diff(dtResposta, dtAvaliacao)) AS medianTempoTespostaAvaliacao,

          count(dtResposta) / count( idPedidoAvaliado) AS pctRespostaAvaliacao

  FROM tb_pedidos_avaliacoes

  GROUP BY ALL

)

SELECT '2018-01-01' AS dtSafra,
        *

FROM tb_group
