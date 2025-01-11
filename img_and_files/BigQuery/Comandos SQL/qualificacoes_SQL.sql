CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Qualificacoes` AS
SELECT qual.`CODIGO` AS ID, qual.`DESCRICAO` AS Descricao
FROM `tlb-dados-abertos.BD_CNPJ.QUALSCSV*` AS qual