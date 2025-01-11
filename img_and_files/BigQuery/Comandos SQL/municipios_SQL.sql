CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Municipios` AS
SELECT muni.`CODIGO` AS ID, muni.`DESCRICAO` AS Municipio
FROM `tlb-dados-abertos.BD_CNPJ.MUNICCSV*` AS muni