-- Databricks notebook source
WITH tb_pedido AS (

    SELECT t1.idPedido,
          t2.idVendedor,
          t2.vlFrete,
          t2.vlPreco,
          t3.*

    FROM silver.olist.pedido as t1

    LEFT JOIN silver.olist.item_pedido AS t2
    on t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.produto AS t3
    ON t3.idProduto = t2.idProduto

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= date('2018-01-01') - INTERVAL 1 YEAR
    AND t2.idVendedor is not null

),

tb_group AS (

  SELECT idVendedor,
        avg(coalesce(nrTamanhoNome,0)) AS avgTamanhoNome,
        min(coalesce(nrTamanhoNome,0)) AS minTamanhoNome,
        max(coalesce(nrTamanhoNome,0)) AS maxTamanhoNome,
        median(coalesce(nrTamanhoNome,0)) AS medianTamanhoNome,

        avg(coalesce(nrFotos,0)) AS avgFotos,
        min(coalesce(nrFotos,0)) AS minFotos,
        max(coalesce(nrFotos,0)) AS maxFotos,
        median(coalesce(nrFotos,0)) AS medianFotos,

        avg(coalesce(nrTamanhoDescricao,0)) AS avgTamanhoDescricao,
        min(coalesce(nrTamanhoDescricao,0)) AS minTamanhoDescricao,
        max(coalesce(nrTamanhoDescricao,0)) AS maxTamanhoDescricao,
        median(coalesce(nrTamanhoDescricao,0)) AS medianTamanhoDescricao,

        count(distinct descCategoria) AS qtdCategorias,

        count(case when descCategoria = 'cama_mesa_banho' then idPedido end) / COUNT(*) AS qtde_cama_mesa_banho,
        count(case when descCategoria = 'moveis_decoracao' then idPedido end) / COUNT(*) AS qtde_moveis_decoracao,
        count(case when descCategoria = 'esporte_lazer' then idPedido end) / COUNT(*) AS qtde_esporte_lazer,
        count(case when descCategoria = 'beleza_saude' then idPedido end) / COUNT(*) AS qtde_beleza_saude,
        count(case when descCategoria = 'informatica_acessorios' then idPedido end) / COUNT(*) AS qtde_informatica_acessorios,
        count(case when descCategoria = 'utilidades_domesticas' then idPedido end) / COUNT(*) AS qtde_utilidades_domesticas,
        count(case when descCategoria = 'brinquedos' then idPedido end) / COUNT(*) AS qtde_brinquedos,
        count(case when descCategoria = 'ferramentas_jardim' then idPedido end) / COUNT(*) AS qtde_ferramentas_jardim,
        count(case when descCategoria = 'cool_stuff' then idPedido end) / COUNT(*) AS qtde_cool_stuff,
        count(case when descCategoria = 'relogios_presentes' then idPedido end) / COUNT(*) AS qtde_relogios_presentes,
        count(case when descCategoria = 'telefonia' then idPedido end) / COUNT(*) AS qtde_telefonia,
        count(case when descCategoria = 'perfumaria' then idPedido end) / COUNT(*) AS qtde_perfumaria,
        count(case when descCategoria = 'automotivo' then idPedido end) / COUNT(*) AS qtde_automotivo,
        count(case when descCategoria = 'bebes' then idPedido end) / COUNT(*) AS qtde_bebes,
        count(case when descCategoria = 'fashion_bolsas_e_acessorios' then idPedido end) / COUNT(*) AS qtde_fashion_bolsas_e_acessorios,
        count(case when descCategoria = 'papelaria' then idPedido end) / COUNT(*) AS qtde_papelaria,

        1000 * sum(vlFrete) / sum( vlAlturaCm * vlComprimentoCm * vlLarguraCm ) AS nrFreteCubagem,
        sum(vlFrete) / sum(vlPesoGramas) AS nrFreteCubagem,
        avg(vlAlturaCm * vlComprimentoCm * vlLarguraCm) AS avgCubagem,
        avg(vlPesoGramas) AS avgPeso

  FROM tb_pedido

  GROUP BY idVendedor

),

tb_count AS (

  SELECT 
        descCategoria,
        COUNT(*) AS qtde

  FROM tb_pedido
  GROUP BY descCategoria
  ORDER BY count(*) desc

),

tb_pct_venda_categoria (

  SELECT *,
        (select sum(qtde) FROM tb_count) AS total,
        qtde / (select sum(qtde) FROM tb_count) as pct,
        sum(qtde / (select sum(qtde) FROM tb_count)) OVER (PARTITION BY 1 ORDER BY qtde desc) AS pctAcum

  FROM tb_count

  where descCategoria is not null

  qualify pctAcum <= 0.82
  
),

tb_vendedor_categoria AS (

    SELECT idVendedor,
          descCategoria,
          count(*) AS qtdProduto,
          sum(vlPreco) AS qtdReceita
          
    FROM tb_pedido

    GROUP BY idVendedor, descCategoria
    ORDER BY idVendedor, descCategoria

),

tb_top_categoria AS (

    SELECT *,
          row_number() over (partition by idVendedor order by qtdProduto desc) AS rankProdutoQtde,
          row_number() over (partition by idVendedor order by qtdReceita desc) AS rankProdutoReceita

    FROM tb_vendedor_categoria

)

SELECT t1.*,
       t2.descCategoria AS topCategoriaVenda,
       t3.descCategoria AS topCategoriaReceita

FROM tb_group as t1

LEFT JOIN tb_top_categoria AS t2
ON t1.idVendedor = t2.idVendedor
AND t2.rankProdutoQtde = 1

LEFT JOIN tb_top_categoria AS t3
ON t1.idVendedor = t3.idVendedor
AND t3.rankProdutoReceita = 1
