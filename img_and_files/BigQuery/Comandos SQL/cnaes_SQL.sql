CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Cnaes` AS
SELECT cnaes.`CODIGO` AS ID, cnaes.`DESCRICAO` AS Descricao
FROM `tlb-dados-abertos.BD_CNPJ.CNAE*` AS cnaes