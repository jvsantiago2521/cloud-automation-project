import os
import functions_framework
import geral
import json
import base64
from google.cloud import bigquery
from google.cloud import pubsub_v1
from datetime import datetime, timedelta  # Importa datetime para manipulação de datas
from pytz import timezone  # Importa timezone para lidar com fusos horários

@functions_framework.cloud_event
def main_function(cloud_event):
    
    project_name = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    project_name = "PORTAL TRANSPARENCIA DESP EMP"

    #Carrega os jsons que serão usados no codigo, que estão presentes no BQ.
    data_geral_config = json.loads(geral.content_json("geral_config", project_name))
    data_tsv_config = json.loads(geral.content_json("tsv_config", project_name))
    data_storage_config = json.loads(geral.content_json("storage_config", project_name))
    data_bq_config = json.loads(geral.content_json("bq_config", project_name))
    data_zip_files_config = json.loads(geral.content_json("zip_files_config", project_name))
    data_csv_files_config = json.loads(geral.content_json("csv_files_config", project_name))

    #Aloca os valores do json em variaveis.
    bucket_name = data_geral_config['bucket_name']
    links_tsv = data_tsv_config['links_tsv']
    tsv_path = data_tsv_config['tsv_path']
    downloaded_files_path = data_storage_config['downloaded_files_path']
    filtered_files_path = data_storage_config['filtered_files_path']
    dataset_name = data_bq_config['dataset_name']
    change_name_position_1 = data_bq_config['change_name_position_1']
    change_name_position_2 = data_bq_config['change_name_position_2']
    allowed_extensions = data_storage_config['allowed_extensions']

    # Obtém a data atual no fuso horário 'America/Sao_Paulo'
    current_date = datetime.now().astimezone(timezone('America/Sao_Paulo')).date()

    # Executa uma query para obter a data e o estado de execuções anteriores
    bigquery_date = geral.query_execution_manager(project_name).Data
    state_executions = geral.query_execution_manager(project_name).Estado

    # Verifica se o estado das execuções anteriores é 3
    if state_executions == 3:
        # Lê a tabela do BigQuery para obter o arquivo a ser filtrado
        file_to_filter = geral.read_bigquery_table(0, project_name)

        #Se existir um arquivo a ser filtrado
        if file_to_filter:
            #Atualiza o estado de 0 (Novo) para 1 (Filtrando)
            geral.update_state_file(file_to_filter, 1)
            #Executa a filtragem
            geral.filter_files(bucket_name, file_to_filter, downloaded_files_path, filtered_files_path, data_zip_files_config, allowed_extensions)
            #Atualiza o estado de 1 (Filtrando) para 2 (Filtrado)
            geral.update_state_file(file_to_filter, 2)

        #Se NAO existir um arquivo a ser filtrado
        else:
            #Se a filtragem foi completa (Todos os arquivos estao com o estado 2)
            if geral.check_filtering_complete(project_name):

                print("All files have been filtered, starting to load!")
                #Limpa o dataset com tabelas antigas
                #geral.clear_dataset(dataset_name)
                #Lista os arquivos presentes na pasta de arquivos filtrados (FilteredFiles)
                csv_list = geral.list_files(bucket_name, filtered_files_path.rstrip('/'))
                print(csv_list)
                #lista = [(datetime(2024, 1, 1) + timedelta(days=i)).strftime('%Y%m%d') for i in range((datetime(2024, 4, 8) - datetime(2024, 1, 1)).days + 1)]
                #For para cada item nessa lista de arquivos filtrados
                for item in csv_list:
                    try:
                        #Adiciona no dataset uma tabela para cada arquivo presente no for
                        geral.add_table(dataset_name, filtered_files_path.rstrip('/') ,item, bucket_name, data_csv_files_config, change_name_position_1, change_name_position_2)
                    #Se a carga der errado e o motivo for caracteres invalidos, o arquivo em questao sera tratado
                    except Exception as e:
                        if "Bad character (ASCII 0) encountered" in str(e):
                            print("\nStarting file treatment...")
                            print(geral.process_csv_file(bucket_name, filtered_files_path.rstrip('/'), item))
                            print("\nTrying to add a table from the treated file...")
                            try:
                                #Tenta adicionar a tabela novamente com o arquivo filtrado
                                geral.add_table(dataset_name, filtered_files_path.rstrip('/') ,item, bucket_name, data_csv_files_config)
                            except Exception as e:
                                print(f"Error adding table from file {item}: {e}")
                #Atualiza o estado para 4, indicando o fim desse processo
                geral.update_date_state(4, current_date, project_name)
                        
            else: 
                
                print("There are files still being filtered!")
                pass
    
    else:
        print("The state of the state machine does not correspond to the state of this process!")
