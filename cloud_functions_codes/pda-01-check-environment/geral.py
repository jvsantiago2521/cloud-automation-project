from google.cloud import bigquery #Importa o cliente do BigQuery da Google Cloud
import json #Importa o módulo json para manipulação de JSON
from google.cloud import bigquery  #Importa o cliente do BigQuery da Google Cloud
from google.cloud import storage  #Importa o cliente do Storage da Google Cloud

project_id = "tlb-dados-abertos"
bigquery_client = bigquery.Client(project=project_id)
storage_client = storage.Client(project=project_id)

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


#Função para criar arquivos TSV no bucket do Storage
def create_tsv(links_tsv, bucket_name, file_path, new_file_name):
    """
    Parâmetros:
    links (list): Uma lista de links a serem incluídos no arquivo TSV.
    bucket_name (str): O nome do bucket do Storage onde o arquivo será salvo.
    file_path (str): O caminho dentro do bucket onde o arquivo será salvo.
    new_file_name (str): O nome do novo arquivo TSV.
    """
    
    #Obtém o bucket do Storage
    bucket = storage_client.bucket(bucket_name)
    #Inicializa o conteúdo CSV como string vazia
    content_csv = ""
    
    #Para cada link nos links fornecidos
    for link in links_tsv:
        #Adiciona o link ao conteúdo CSV
        content_csv += f"{link}\n"
        
    #Cria um blob no bucket do Storage com o caminho especificado e faz o upload do conteúdo CSV
    blob = bucket.blob(f"{file_path}{new_file_name}.tsv")
    blob.upload_from_string(f"TsvHttpData-1.0\n{content_csv}", content_type="text/tab-separated-values")
    blob.make_public()

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


def clean_machine_state(project_name):

    query1 = f"""
        UPDATE `tlb-dados-abertos.COMANDO.gestor_execucoes`
        SET Estado = 0
        WHERE Projeto = '{project_name}'
    """

    query2 = f"""
        UPDATE `tlb-dados-abertos.COMANDO.gestor_arquivos`
        SET estado = 0
        WHERE projeto = '{project_name}'
    """
    
    #Executa a query
    bigquery_client.query(query1)
    bigquery_client.query(query2)