-- Databricks notebook source
CREATE TABLE analytics.asn.abt_churn_2023

with tb_fs_referencia AS (

  SELECT dtSafra,
        idVendedor,
        nrRecencia
  FROM analytics.asn.seller_pedidos
  WHERE nrRecencia < 30

),

tb_dia_pedido AS (
  SELECT 
        DISTINCT
        t2.idVendedor,
        date(t1.dtPedido) AS datePedido

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido
),

tb_join AS (

  SELECT t1.*,
        t2.datePedido

  FROM tb_fs_referencia AS t1

  LEFT JOIN tb_dia_pedido AS t2
  ON t1.idVendedor = t2.idVendedor
  AND t1.dtSafra <= t2.datePedido
  AND t2.datePedido < t1.dtSafra + interval 30 day

),

tb_group AS (

  SELECT 
        dtSafra,
        idVendedor,
        nrRecencia,
        min(datePedido) AS dt_prox_venda

  FROM tb_join

  GROUP BY ALL

),

tb_churn (

  SELECT 
    *,
    CASE WHEN dt_prox_venda IS NOT NULL THEN 0 ELSE 1 END AS flChurnMes,
    
    CASE WHEN dt_prox_venda IS NULL THEN 1
        WHEN datediff(dt_prox_venda,dtSafra) > 30 - nrRecencia THEN 1
        ELSE 0
    END AS flChurnD30
    
  FROM tb_group

)

SELECT
      t1.dtSafra,
      t1.idVendedor,
      dt_prox_venda,
      flChurnMes,
      flChurnD30,
      avgNotaAvaliacao,
      pctPedidoAvaliados,
      descAvaliacaoBaixaPedido,
      descAvaliacaoNeutraPedido,
      descAvaliacaoAltaPedido,
      descAvaliacaoBaixaPedidoAvaliado,
      descAvaliacaoNeutraPedidoAvaliado,
      descAvaliacaoAltaPedidoAvaliado,
      avgTempoTespostaAvaliacao,
      medianTempoTespostaAvaliacao,
      pctRespostaAvaliacao,
      qtdBoleto,
      qtdCredit_card,
      qtdVoucher,
      qtdDebit_card,
      avgParcelas,
      avgParcelasCartao,
      avgTiposPagamento,
      nrIdade,
      t4.nrRecencia,
      nrQtdePedido,
      nrQtdeItensTotais,
      nrQtdeItensDistintos,
      nrReceita,
      nrTicketMedio,
      nrValorMedioItem,
      pctFreteValor,
      nrQtdeItemPedido,
      nrMediaVendaMes,
      pctMesesAtivacao,
      nrDiasPedido,
      pctPedidoAC,
      pctPedidoAL,
      pctPedidoAM,
      pctPedidoAP,
      pctPedidoBA,
      pctPedidoCE,
      pctPedidoDF,
      pctPedidoES,
      pctPedidoGO,
      pctPedidoMA,
      pctPedidoMG,
      pctPedidoMS,
      pctPedidoMT,
      pctPedidoPA,
      pctPedidoPB,
      pctPedidoPE,
      pctPedidoPI,
      pctPedidoPR,
      pctPedidoRJ,
      pctPedidoRN,
      pctPedidoRO,
      pctPedidoRR,
      pctPedidoRS,
      pctPedidoSC,
      pctPedidoSE,
      pctPedidoSP,
      pctPedidoTO,
      nrTempoMedioEntrega,
      nrTempoMedioEntregaPromessa,
      nrQtdePedidosAtraso,
      nrMaxVendaMes,
      nrMinVendaMes,
      nrMedianVendaMes,
      nrStdVendaMes,
      nrMaxIntervaloVenda,
      nrMinIntervaloVenda,
      nrAvgIntervaloVenda,
      nrMaxShareQtde,
      nrMaxShareValor,
      avgTamanhoNome,
      minTamanhoNome,
      maxTamanhoNome,
      medianTamanhoNome,
      avgFotos,
      minFotos,
      maxFotos,
      medianFotos,
      avgTamanhoDescricao,
      minTamanhoDescricao,
      maxTamanhoDescricao,
      medianTamanhoDescricao,
      qtdCategorias,
      qtde_cama_mesa_banho,
      qtde_moveis_decoracao,
      qtde_esporte_lazer,
      qtde_beleza_saude,
      qtde_informatica_acessorios,
      qtde_utilidades_domesticas,
      qtde_brinquedos,
      qtde_ferramentas_jardim,
      qtde_cool_stuff,
      qtde_relogios_presentes,
      qtde_telefonia,
      qtde_perfumaria,
      qtde_automotivo,
      qtde_bebes,
      qtde_fashion_bolsas_e_acessorios,
      qtde_papelaria,
      nrFreteCubagem,
      nrFretePeso,
      avgCubagem,
      avgPeso,
      topCategoriaVenda,
      topCategoriaReceita


FROM tb_churn AS t1

LEFT JOIN analytics.asn.seller_avaliacoes AS t2
ON t1.idVendedor = t2.idvendedor
AND t1.dtSafra = t2.dtSafra

LEFT JOIN analytics.asn.seller_pagamento AS t3
ON t1.idVendedor = t3.idvendedor
AND t1.dtSafra = t3.dtSafra

LEFT JOIN analytics.asn.seller_pedidos AS t4
ON t1.idVendedor = t4.idvendedor
AND t1.dtSafra = t4.dtSafra

LEFT JOIN analytics.asn.seller_produto AS t5
ON t1.idVendedor = t5.idvendedor
AND t1.dtSafra = t5.dtSafra

-- COMMAND ----------

SELECT * FROM analytics.asn.abt_churn_2023
