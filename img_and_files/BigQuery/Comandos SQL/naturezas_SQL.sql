CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Naturezas` AS
SELECT natju.`CODIGO` AS ID, natju.`DESCRICAO` AS Descricao
FROM `tlb-dados-abertos.BD_CNPJ.NATJUCSV*` AS natju