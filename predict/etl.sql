SELECT
      t1.dtSafra,
      t1.idVendedor,
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
      nrRecencia,
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

FROM analytics.asn.seller_pedidos AS t1

LEFT JOIN analytics.asn.seller_avaliacoes AS t2
ON t1.idVendedor = t2.idvendedor
AND t1.dtSafra = t2.dtSafra

LEFT JOIN analytics.asn.seller_pagamento AS t3
ON t1.idVendedor = t3.idvendedor
AND t1.dtSafra = t3.dtSafra

LEFT JOIN analytics.asn.seller_produto AS t5
ON t1.idVendedor = t5.idvendedor
AND t1.dtSafra = t5.dtSafra

WHERE t1.dtSafra = '{date}'