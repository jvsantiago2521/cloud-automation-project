INSERT INTO `tlb-dados-abertos.ANATEL.SCM-Historico` (
  Data,
  Grupo_Economico,
  Empresa,
  CNPJ,
  Porte,
  UF,
  Municipio,
  Cod_IBGE,
  Velocidade,
  Tecnologia,
  Meio_Acesso,
  Tipo_Pessoa,
  Qt_Acessos,
  BR_UF,
  Banda_Total,
  Faixa_Velocidade
)
SELECT 
  CAST(SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(Ano, '-', `Mês`, '-01')) AS DATE) AS Data,
  `Grupo Econômico` AS Grupo_Economico,
  Empresa, 
  FORMAT('%s.%s.%s/%s-%s', SUBSTR(CNPJ, 1, 2), SUBSTR(CNPJ, 3, 3), SUBSTR(CNPJ, 6, 3), SUBSTR(CNPJ, 9, 4), SUBSTR(CNPJ, 13, 2)) AS CNPJ,
  `Porte da Prestadora` AS Porte,
  UF,
  `Município` AS Municipio,
  `Código IBGE Município` AS Cod_IBGE,
  CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) AS Velocidade,
  Tecnologia,
  `Meio de Acesso` AS Meio_Acesso,
  `Tipo de Pessoa` AS Tipo_Pessoa,
  CAST(Acessos AS INT64) AS Qt_Acessos,
  CONCAT('BR-', UF) AS BR_UF,
  CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) * CAST(Acessos AS FLOAT64) AS Banda_Total,
  CASE
    WHEN CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) >= 0 AND CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) < 1 THEN '1. Menor que 1 Mbps'
    WHEN CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) >= 1 AND CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) < 10 THEN '2. De 1 Mbps a 10 Mbps'
    WHEN CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) >= 10 AND CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) < 100 THEN '3. De 10 Mbps a 100 Mbps'
    WHEN CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) >= 100 AND CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) < 1000 THEN '4. De 100 Mbps a 1 Gbps'
    WHEN CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) >= 1000 AND CAST(REPLACE(Velocidade, ',', '.') AS FLOAT64) < 10000 THEN '5. De 1 Gbps a 10 Gbps'
    ELSE '6. Maior que 10 Gbps'
  END AS Faixa_Velocidade
FROM 
  `tlb-dados-abertos.BD_ANATEL.Acessos_Banda_Larga_Fixa_2024csv` AS src
WHERE 
  CAST(SAFE.PARSE_DATE('%Y-%m-%d', CONCAT(src.Ano, '-', `Mês`, '-01')) AS DATE) > (
    SELECT MAX(Data) 
    FROM `tlb-dados-abertos.ANATEL.SCM-Historico`
  )