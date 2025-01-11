import functions_framework  #Importa o framework para criar funções Cloud
import json  #Importa o módulo json para manipulação de JSON
import geral  #Importa o módulo geral contendo funções auxiliares
from datetime import datetime  #Importa datetime para manipulação de datas
from pytz import timezone  #Importa timezone para lidar com fusos horários
import base64  #Importa base64 para decodificar mensagens codificadas


@functions_framework.cloud_event
def main_function(cloud_event):
    #Variável que recebe o nome do projeto a ser executado por meio da mensagem do PubSub.
    project_name = base64.b64decode(cloud_event.data["message"]["data"]).decode()

    #Carrega os jsons que serão usados no codigo, que estão presentes no BQ.
    data_geral_config = json.loads(geral.content_json("geral_config", project_name))
    data_tsv_config = json.loads(geral.content_json("tsv_config", project_name))

    #Aloca os valores do json em variaveis.
    bucket_name = data_geral_config['bucket_name']
    links_tsv = data_tsv_config['links_tsv']
    tsv_path = data_tsv_config['tsv_path']
    date_link_y_m = data_tsv_config['date_link_y_m']
    date_link_y_m_d = data_tsv_config['date_link_y_m_d']

    #Obtem a data atual.
    current_date = datetime.now().astimezone(timezone('America/Sao_Paulo')).date()

    #Obtem a data da ultima execução e o estado no gerenciador de execuções.
    bigquery_date = geral.query_execution_manager(project_name).Data
    state_executions = geral.query_execution_manager(project_name).Estado

    #Se a ultima execução deste processo de verificar ambiente não foi no dia atual, este processo é executado.
    if current_date != bigquery_date:
        if date_link_y_m:
            for link in links_tsv:
               updated_link = link + "/" + current_date.strftime('%Y%m')
            links_tsv = []
            links_tsv.append(updated_link)
        elif date_link_y_m_d:
            for link in links_tsv:
               updated_link = link + "/" + current_date.strftime('%Y%m%d')
            links_tsv = []
            links_tsv.append(updated_link)
        else:
            pass
        geral.clean_machine_state(project_name)
        #Cria o tsv
        geral.create_tsv(links_tsv, bucket_name, tsv_path, f"{project_name.lower().replace(' ', '_')}_links")
        #Atualiza a data de execução para o dia atual e o atualiza o estado para o proximo.
        geral.update_date_state(1, current_date, project_name)
        print("Environment check completed!")
    else:
        print("This process has already been executed on the current date!")