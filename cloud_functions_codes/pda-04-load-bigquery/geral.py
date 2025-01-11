from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime, timedelta
import zipfile
import io
import os
import csv
import json
import time
import re

project_id = "tlb-dados-abertos"
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

buffer_limit_MB = 1024

#Função para obter o conteúdo JSON de uma coluna no BigQuery
def content_json(column, project_name):
    """
    Parâmetros:
    column (str): O nome da coluna a ser consultada (Cada coluna é um tipo de json).
    project_name (str): O nome do projeto para filtrar na consulta.
    """

    #Define a query para obter o conteúdo JSON da coluna especificada
    query = f"""
        SELECT {column} FROM tlb-dados-abertos.COMANDO.json
        WHERE Projeto = '{project_name}';
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)
    #Obtém o resultado da query
    result = next(query_job.result(), None)
    #Obtém o conteúdo JSON da coluna especificada
    content_json = getattr(result, column)
    #Retorna o conteúdo JSON
    return content_json

# Define uma função para filtrar arquivos em um bucket do Google Cloud Storage
def filter_files(bucket_name, zip_file_name, source_folder, destination_folder, data_zip_files_config, allowed_extensions):
    
    name_changes_daily = data_zip_files_config["name_changes_daily"]

    if name_changes_daily:
        codification = data_zip_files_config["codification"]
    else:
        codification = data_zip_files_config[zip_file_name]["codification"]
    try:
        # Inicializa o cliente do Cloud Storage
        client = storage.Client()

        # Obtém o bucket
        bucket = client.get_bucket(bucket_name)

        # Obtém o blob do arquivo ZIP
        blob = bucket.blob(f"{source_folder}{zip_file_name}")

        print(blob)

        # Verifica se o blob existe
        if blob.exists():
            # Faz o download do arquivo ZIP
            zip_file = io.BytesIO(blob.download_as_bytes())

            # Extrai o conteúdo do arquivo ZIP
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            
                for internal_file in zip_ref.namelist():
                    file_extension = os.path.splitext(internal_file)[1].lower()
                    print(file_extension)
                    print(internal_file)

                    if file_extension in allowed_extensions:
                        # Obtém o blob de destino para o arquivo CSV
                        destination_blob = bucket.blob(f"{destination_folder}{internal_file}")

                        # Define o limite de tamanho de cada parte em bytes (1000 MB)
                        part_size_limit = buffer_limit_MB * 1024 * 1024

                        # Inicializa o buffer para armazenar temporariamente os dados
                        buffer = io.BytesIO()
                        
                        # Inicializa o contador de linhas
                        line_count = 0

                        # Itera sobre o conteúdo do arquivo CSV no ZIP
                        with zip_ref.open(internal_file) as csv_file:
                            while True:
                                # Lê uma linha do arquivo
                                line = csv_file.readline()

                                # Se a linha é vazia, significa que chegamos ao final do arquivo
                                if not line:
                                    break

                                try:
                                    decoded_line = line.decode(codification).encode('utf-8')
                                except UnicodeDecodeError as e:
                                    print(f"Erro ao decodificar a linha: {e}")
                                    continue

                                # Incrementa o contador de linhas
                                line_count += 1

                                # Adiciona a linha ao buffer
                                buffer.write(decoded_line)

                                # Verifica se o tamanho do buffer está próximo do limite e se a próxima linha excede o limite
                                if buffer.tell() >= 0.9 * part_size_limit and len(csv_file.peek()) >= part_size_limit:
                                    # Faz upload do conteúdo atual para o destino
                                    buffer.seek(0)
                                    destination_blob.upload_from_file(buffer, content_type='text/csv', rewind=True)
                                    
                                    # Limpa o buffer
                                    buffer = io.BytesIO()
                                    
                                    # Reinicia o contador de linhas
                                    line_count = 0

                            # Após a última leitura do arquivo, faz o último upload se houver dados no buffer
                            if buffer.tell() > 0:
                                buffer.seek(0)
                                destination_blob.upload_from_file(buffer, content_type='text/csv', rewind=True)
                                buffer.close()
                    else:
                        print(f"Ignorando arquivo com extensão não permitida: {internal_file}")

            print("File extracted and converted successfully")
        else:
            print(f"The file doesn't exist or is not a .zip file")
    except Exception as e:
        print(f"An error occurred: {e}")


# Define uma função para ler a tabela no BigQuery
def read_bigquery_table(state, project_name):
    try:
        # Inicializa o cliente BigQuery
        client = bigquery.Client(project=project_id)
        
        QUERY = (
            f"""
            SELECT arquivo FROM `tlb-dados-abertos.COMANDO.gestor_arquivos`
            WHERE estado = {state} AND projeto = '{project_name}'
            LIMIT 1
            """
        )
        # Executa a query
        query_job = client.query(QUERY)

        for result in query_job.result():
            resultado = result.get('arquivo')

        return resultado
    except Exception as e:
        print(e)
        return False

# Define uma função para atualizar o estado do arquivo no BigQuery
def update_state_file(arquivo, novo_estado):
    try:
        print(arquivo, novo_estado)
        # Inicializa o cliente BigQuery
        client = bigquery.Client(project=project_id)
        # Define a query para atualizar o estado do arquivo
        QUERY = (
            'UPDATE `tlb-dados-abertos.COMANDO.gestor_arquivos` '
            f'SET estado = {novo_estado} '
            f'WHERE arquivo = "{arquivo}" '
        )

        query_job = client.query(QUERY)
        query_job.result()  # Aguarda a conclusão da consulta
        
        print("Estado atualizado com sucesso!!!")
    except Exception as e:
        print(f"Error in atualizar_estado_bigquery: {e}")

def create_dataset(dataset_name):
    # Define o ID completo do dataset
    dataset_ref = bigquery_client.dataset(dataset_name)

    # Tenta excluir o dataset existente
    bigquery_client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)

    # Configura as opções do novo dataset
    new_dataset = bigquery.Dataset(dataset_ref)

    # Tenta criar o novo dataset
    new_dataset = bigquery_client.create_dataset(new_dataset)
    print(f"Dataset {new_dataset.dataset_id} criado com sucesso.")

def add_table(dataset_name, folder_path, arquivo, bucket_name, data_csv_files_config, change_name_position_1, change_name_position_2):

    #lista = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y%m%d') for i in range((datetime(2024, 4, 8) - datetime(2024, 1, 1)).days + 1)]
    #if arquivo.split("_")[0] in lista:
        #return

    same_schema = data_csv_files_config["same_schema"]
    complete_suffix = data_csv_files_config["complete_suffix"]

    temp = arquivo

    if same_schema:
        schema = data_csv_files_config["schema"]
        field_delimiter = data_csv_files_config["field_delimiter"] 
        quote_character = data_csv_files_config["quote_character"]
        skip_leading_rows = data_csv_files_config["skip_leading_rows"]
    else:
        if complete_suffix:
            arquivo = arquivo[arquivo.index('_'):]
        schema = data_csv_files_config[arquivo]["schema"]
        field_delimiter = data_csv_files_config[arquivo]["field_delimiter"] 
        quote_character = data_csv_files_config[arquivo]["quote_character"]
        skip_leading_rows = data_csv_files_config[arquivo]["skip_leading_rows"]

    arquivo = temp
    
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()

    # Obtenha o bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Crie o ID da tabela
    name_new_table = corrigir_nome(arquivo, change_name_position_1, change_name_position_2)
    table_id = f"{project_id}.{dataset_name}.{name_new_table}"

    try:
        schema = [{"name": coluna, "type": "STRING"} for coluna in schema]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            skip_leading_rows=skip_leading_rows,  # Ignorar a linha do cabeçalho
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=field_delimiter,  # Delimitador de campo
            quote_character=quote_character,  # Caractere de citação
            encoding='UTF-8',  # Codificação do arquivo
        )

        # Construa a URL do arquivo CSV no Google Cloud Storage
        file_path = f"gs://tlb_dados_abertos/{folder_path}/{arquivo}"

        # Carregar o arquivo CSV para o BigQuery
        load_job = client.load_table_from_uri(
            source_uris=[file_path],
            destination=table_id,
            job_config=job_config,
        )

        # Aguarde a conclusão do trabalho de carregamento
        load_job.result()
    except Exception as e:
        print(e)

    print(f"Tabela {table_id} criada com sucesso a partir do arquivo {arquivo}.")

def list_files(bucket_name, folder):
    # Lista para armazenar os nomes dos arquivos
    file_list = []
    # Inicializa o cliente do Cloud Storage
    client = storage.Client()
    # Obtém o bucket
    bucket = client.get_bucket(bucket_name)
    # Lista os blobs dentro da pasta específica
    blobs = bucket.list_blobs(prefix=folder)
    # Itera sobre os blobs e adiciona os nomes dos arquivos à lista
    for blob in blobs:
        file_name = os.path.basename(blob.name)
        # Verifica se o nome do arquivo não está vazio antes de adicionar à lista
        if file_name.strip():  # Verifica se o nome do arquivo não é apenas espaços em branco
            file_list.append(file_name)
    return file_list

def clear_dataset(dataset_name):

    dataset_ref = bigquery_client.dataset(dataset_name)

    tables = list(bigquery_client.list_tables(dataset_ref))

    for table in tables:
        print(table.table_id)
        #Define a query para obter a data e o estado da execução
        query = f"""
            DROP TABLE `tlb-dados-abertos.{dataset_name}.{table.table_id}`;
        """
        
        #Executa a query
        query_job = bigquery_client.query(query)
        #Obtém o resultado da query
        result = next(query_job.result(), None)

# Define uma função para ler a tabela no BigQuery
def check_filtering_complete(project_name):
    try:
        # Inicializa o cliente BigQuery
        client = bigquery.Client(project=project_id)
        
        QUERY = (
            'SELECT arquivo FROM `tlb-dados-abertos.COMANDO.gestor_arquivos` '
            f'WHERE estado != 2 AND projeto = "{project_name}"'
            'LIMIT 1'
            )
        # Executa a query
        query_job = client.query(QUERY)
        results = list(query_job)

        if results:
            return False
        else:
            return True
    except Exception as e:
        print(e)
        return False

def process_csv_file(bucket_name, folder_path, file_name):
    
    # Inicializando o cliente do Storage
    storage_client = storage.Client()
    
    # Obtendo o arquivo do bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(folder_path + '/' + file_name)
    
    # Baixando o conteúdo do arquivo como bytes
    content = blob.download_as_bytes()
    
    # Removendo caracteres NUL do conteúdo
    content_cleaned = remove_null_character(content)
    
    # Sobrescrevendo o conteúdo do arquivo original
    blob.upload_from_string(content_cleaned, content_type='text/csv')

    # Fazendo log da conclusão do processo
    return f'Arquivo tratado em: gs://{bucket_name}/{folder_path}/{file_name}'

def remove_null_character(content):
    # Implemente a lógica para remover caracteres NUL do conteúdo aqui
    # Por exemplo:
    return content.replace(b'\x00', b'')

#Função para atualizar a data e o estado da execução no BigQuery
def update_date_state(next_state, current_date, project_name):
    """
    Parâmetros:
    next_state (int): O número correspondente ao próximo estado.
    current_date (datetime.date): A data atual.
    project_name (str): O nome do projeto para filtrar na consulta.
    """

    #Define a query para atualizar a data e o estado da execução
    query = f"""
        UPDATE tlb-dados-abertos.COMANDO.gestor_execucoes
        SET Estado = {next_state}, Data = '{current_date}'
        WHERE Projeto = '{project_name}'
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)

#Função para obter a data e o estado da execução do BigQuery
def query_execution_manager(project_name):
    """
    Parâmetros:
    project_name (str): O nome do projeto para filtrar na consulta.
    """
 
    #Define a query para obter a data e o estado da execução
    query = f"""
        SELECT Data, Estado FROM tlb-dados-abertos.COMANDO.gestor_execucoes
        WHERE Projeto = '{project_name}'
    """
    
    #Executa a query
    query_job = bigquery_client.query(query)
    #Obtém o resultado da query
    result = next(query_job.result(), None)
    #Retorna o resultado da query
    return result

def corrigir_nome(name, change_name_position_1, change_name_position_2):

    if change_name_position_1:
        # Dividir a string pelo ponto
        parts = name.split('.')
        
        # Identificar o último e o primeiro ponto
        prefix = parts[0]  # Parte antes do primeiro ponto
        suffix = parts[-1] # Parte depois do último ponto
        middle = parts[1:-1]  # Partes intermediárias entre o primeiro e o último ponto
        
        # Rearranjar as partes
        name = f"{suffix}." + '.'.join(middle) + f".{prefix}"

    if change_name_position_2:
        # Dividir o nome pelo underscore para separar a data e o restante
        parts = name.split('_')

        # A última parte deve ser separada pela extensão (csv)
        last_part_with_extension = parts[-1].split('.')

        # Pegar a data da primeira parte
        date = parts[0]

        name = '_'.join(parts[1:-1] + [last_part_with_extension[0]]) + f"_{date}.csv"
        

    # Expressão regular para permitir apenas letras Unicode, marcas, números, conectores, traços e espaços
    padrao = r'[^\w\s\u00C0-\u017F\-]'
    name_corrigido = re.sub(padrao, '', name)
    
    return name_corrigido