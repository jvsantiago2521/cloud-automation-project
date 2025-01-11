CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Motivos` AS
SELECT moti.`CODIGO` AS ID, moti.`DESCRICAO` AS Descricao
FROM `tlb-dados-abertos.BD_CNPJ.MOTICSV*` AS moti