INSERT INTO `tlb-dados-abertos.PortalTransparencia.ExecucaoDaDespesa`
SELECT
  *
FROM
  `tlb-dados-abertos.BD_PORT_TRANSP_EXEC_DESP.2*`
WHERE
  Ano_e_mes_do_lancamento = (
  SELECT
    MAX(Ano_e_mes_do_lancamento)
  FROM
    `tlb-dados-abertos.BD_PORT_TRANSP_EXEC_DESP.2*`)
EXCEPT DISTINCT
SELECT
  *
FROM
  `tlb-dados-abertos.PortalTransparencia.ExecucaoDaDespesa`;