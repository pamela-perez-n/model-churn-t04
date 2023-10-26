-- Databricks notebook source
WITH tb_pedidos (
    SELECT t1.idPedido,
          t2.idVendedor,
          t3.idPagamentoPedido,
          t3.descTipoPagamento,
          t3.nrParcelas,
          t3.vlPagamento       

    FROM silver.olist.pedido AS t1

    LEFT JOIN silver.olist.item_pedido AS t2
    ON t1.idPedido = t2.idPedido

    LEFT JOIN silver.olist.pagamento_pedido AS t3
    ON t1.idPedido = t3.idPedido

    WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= date('2018-01-01') - INTERVAL 1 YEAR
    AND idVendedor is not null
)

SELECT idVendedor,
        count(distinct case when descTipoPagamento = 'boleto' then idPedido end) / count(distinct idPedido) as qtdBoleto,
        count(distinct case when descTipoPagamento = 'credit_card' then idPedido end) / count(distinct idPedido) as qtdCredit_card,
        count(distinct case when descTipoPagamento = 'voucher' then idPedido end) / count(distinct idPedido) as qtdVoucher,
        count(distinct case when descTipoPagamento = 'debit_card' then idPedido end) / count(distinct idPedido) as qtdDebit_card,
        sum(nrParcelas) / count(distinct idPedido) AS avgParcelas,
        coalesce(sum(case when descTipoPagamento = 'credit_card' then nrParcelas end) / count(distinct case when descTipoPagamento = 'credit_card' then idPedido end),0) AS avgParcelasCartao,
        count(distinct descTipoPagamento, idPedido) / count(distinct idPedido) AS avgTiposPagamento

FROM tb_pedidos

group by idVendedor
