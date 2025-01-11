CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Paises` AS
SELECT paises.`CODIGO` AS ID, paises.`DESCRICAO` AS Paises
FROM `tlb-dados-abertos.BD_CNPJ.PAISCSV*` AS paises