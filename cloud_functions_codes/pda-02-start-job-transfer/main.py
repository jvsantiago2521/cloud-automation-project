import functions_framework  # Importa o framework para criar funções Cloud
import json  # Importa o módulo json para manipulação de JSON
import base64  # Importa base64 para decodificar mensagens codificadas
from datetime import datetime  # Importa datetime para manipulação de datas
from pytz import timezone  # Importa timezone para lidar com fusos horários
import geral  # Importa o módulo geral contendo funções auxiliares

@functions_framework.cloud_event
def main_function(cloud_event):
    #Variável que recebe o nome do projeto a ser executado por meio da mensagem do PubSub.
    project_name = base64.b64decode(cloud_event.data["message"]["data"]).decode()
    project_name = "PORTAL TRANSPARENCIA DESP EMP"
    
    #Carrega os jsons que serão usados no codigo, que estão presentes no BQ.
    data_geral_config = json.loads(geral.content_json("geral_config", project_name))
    data_tsv_config = json.loads(geral.content_json("tsv_config", project_name))
    data_job_config = json.loads(geral.content_json("job_config", project_name))
    data_storage_config = json.loads(geral.content_json("storage_config", project_name))

    #Aloca os valores do json em variaveis.
    bucket_name = data_geral_config['bucket_name']
    links_tsv = data_tsv_config['links_tsv']
    tsv_path = data_tsv_config['tsv_path']
    job_name = data_job_config['job_name']
    downloaded_files_path = data_storage_config['downloaded_files_path']
    filtered_files_path =  data_storage_config['filtered_files_path']

    #Obtem a data atual.
    current_date = datetime.now().astimezone(timezone('America/Sao_Paulo')).date()

    #Obtem a data da ultima execução e o estado no gerenciador de execuções.
    bigquery_date = geral.query_execution_manager(project_name).Data
    state_executions = geral.query_execution_manager(project_name).Estado

    # Verifica se o estado das execuções é 1 (pronto para executar)
    if state_executions == 1:

        # Obtém o status da última operação de transferência
        status_job = geral.operation_details(job_name)['status'] 

        # Verifica se a última operação de transferência está em andamento
        if status_job == "IN_PROGRESS":
            print("Transfer job is in progress, nothing will happen!")
        else:
            # Obtém a data da última operação de transferência
            last_operation_date = datetime.strptime(geral.operation_details(job_name)['endTime'].split('T')[0], '%Y-%m-%d').date()
            
            # Verifica se a data da última operação é diferente da data atual
            if last_operation_date != current_date:
                try:
                    #geral.delete_files_in_folder(bucket_name, downloaded_files_path)
                    #geral.delete_files_in_folder(bucket_name, filtered_files_path)
                    print("Starting transfer job!!!")
                    #Executa job de transferencia
                    geral.run_job(job_name)
                    print(f'Executed transfer job: {job_name}')
                except Exception as e:
                    print(f'Error executing transfer job: {e}')
            else:
                # Atualiza o estado e a data no BigQuery
                if geral.links_with_error(job_name):
                    print(geral.links_with_error(job_name))
                    geral.add_link_pending_table(project_name, geral.links_with_error(job_name))
                #geral.update_date_state(2, current_date, project_name)
                print("The transfer job was completed, changing the state machine...")
    else:
        print("The state of the state machine does not correspond to the state of this process!")