import functions_framework
import json
import os
import geral
import base64
from google.cloud import bigquery  # Importa o cliente do BigQuery da Google Cloud
from datetime import datetime  # Importa datetime para manipulação de datas
from pytz import timezone  # Importa timezone para lidar com fusos horários

@functions_framework.cloud_event
def main_function(cloud_event):

    #Variável que recebe o nome do projeto a ser executado por meio da mensagem do PubSub.
    project_name = base64.b64decode(cloud_event.data["message"]["data"]).decode()

    #Carrega os jsons que serão usados no codigo, que estão presentes no BQ.
    data_geral_config = json.loads(geral.content_json("geral_config", project_name))
    data_storage_config = json.loads(geral.content_json("storage_config", project_name))

    #Aloca os valores do json em variaveis.
    bucket_name = data_geral_config['bucket_name']
    subfolder_path = data_storage_config['subfolder_path']
    downloaded_files_path = data_storage_config['downloaded_files_path']

    # Obtém a data atual no fuso horário 'America/Sao_Paulo'
    current_date = datetime.now().astimezone(timezone('America/Sao_Paulo')).date()

    # Executa uma query para obter a data e o estado de execuções anteriores
    bigquery_date = geral.query_execution_manager(project_name).Data
    state_executions = geral.query_execution_manager(project_name).Estado

    # Verifica se o estado da execução é 2
    if state_executions == 2:
        # Organiza os arquivos no Google Cloud Storage
        geral.organizer(bucket_name, downloaded_files_path, subfolder_path, project_name)

        file_list = geral.list_files(bucket_name, downloaded_files_path)
        geral.create_files_states(file_list, project_name)

        # Atualiza a data e o estado da execução no BigQuery para indicar que o processo foi concluído
        geral.update_date_state(3, current_date, project_name)
        print('All files have been moved to the correct directory.')
    else:
        print("The state of the state machine does not correspond to the state of this process!")