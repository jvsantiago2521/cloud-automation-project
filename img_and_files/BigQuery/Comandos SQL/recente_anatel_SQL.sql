CREATE OR REPLACE TABLE `tlb-dados-abertos.ANATEL.SCM-Recente` AS
SELECT
  *
FROM
  `tlb-dados-abertos.ANATEL.SCM-Historico`
WHERE
  DATA = (
  SELECT
    MAX(DATA)
  FROM
    `tlb-dados-abertos.ANATEL.SCM-Historico`)