CREATE OR REPLACE TABLE `tlb-dados-abertos.CNPJ.Empresas` AS
SELECT estab.`CNPJ_BASICO` AS CNPJ_Base, estab.`CNPJ_ORDEM` AS CNPJ_Ordem, estab.`CNPJ_DV` AS CNPJ_DV, estab.`IDENTIFICADOR_MATRIZ_FILIAL` AS Matriz_Filial, estab.`NOME_FANTASIA` AS Nome_Fantasia, estab.`CNAE_FISCAL_PRINCIPAL` AS CNAE_Principal, estab.`CNAE_FISCAL_SECUNDARIA` AS CNAEs_Secundarios, estab.`BAIRRO` AS Bairro, estab.`CEP` AS CEP, estab.`UF` AS UF, estab.`MUNICIPIO` AS Municipio, emp.`ENTE_FEDERATIVO_RESPONSAVEL` AS Ente_Federativo, emp.`NATUREZA_JURIDICA` AS Natureza_Juridica, emp.`PORTE_DA_EMPRESA` AS Porte_Empresa, emp.`QUALIFICACAO_DO_RESPONSAVEL` AS Qualific_Resp, emp.`RAZAO_SOCIAL_NOME_EMPRESARIAL` AS Razao_Social,
CONCAT('BR-', estab.UF) AS BR_UF,
SAFE.PARSE_DATE('%Y%m%d', estab.`DATA_SITUACAO_CADASTRAL`) AS Data_Inicio_Atividade,
CASE 
    WHEN estab.IDENTIFICADOR_MATRIZ_FILIAL = '1' THEN 'Matriz'
    WHEN estab.IDENTIFICADOR_MATRIZ_FILIAL = '2' THEN 'Filial'
END AS Desc_Matriz_Filial,
CASE 
    WHEN emp.PORTE_DA_EMPRESA = '00' THEN 'Nao informado'
    WHEN emp.PORTE_DA_EMPRESA = '01' THEN 'Micro empresa'
    WHEN emp.PORTE_DA_EMPRESA = '03' THEN 'Empresa de pequeno porte'
    WHEN emp.PORTE_DA_EMPRESA = '05' THEN 'Demais'
END AS Desc_Porte,
CONCAT(estab.CNPJ_BASICO, '/', estab.CNPJ_ORDEM, '-', estab.CNPJ_DV) AS CNPJ
FROM `tlb-dados-abertos.BD_CNPJ.EMPRECSV*` AS emp, `tlb-dados-abertos.BD_CNPJ.ESTABELE*` AS estab
WHERE estab.`SITUACAO_CADASTRAL` = '02' 
AND estab.`CNPJ_BASICO` = emp.`CNPJ_BASICO`
AND emp.NATUREZA_JURIDICA IN ('1015', '1023', '1031', '1040', '1058', '1066', '1074', '1082', '1104', '1112', 
                              '1120', '1139', '1147', '1155', '1163', '1171', '1180', '1198', '1210', '1228', 
                              '1236', '1244', '1252', '1260', '1279', '1287', '1295', '1309', '1317', '1325', 
                              '1333', '1341', '2011', '2038', '2275')