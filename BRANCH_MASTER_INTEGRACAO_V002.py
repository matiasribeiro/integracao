import pandas as pd
import numpy as np
import collections
import requests
import json
import gzip
import shutil
import pymongo
import time
from selenium import webdriver
from urllib.request import urlopen
from pymongo import MongoClient
import time



def df_contratos_governo_pb(ANO_LICITACAO):
    #------------------------------------------------------------------------------
    # US3710
    # AUTOR: MATIAS RIBEIRO
    #
    # REQUISITOS:
    # Os arquivos com seus devidos nomes (lista de constratos) 
    # devem estar dentro de uma pasta com o nome 'base_de_dados'
    #------------------------------------------------------------------------------
    #
    # COLUNAS:
    # CtNumero --------------------- NUMERO_CONTRATO
    # NU_ProcessoLicitacao --------- NUMERO_PROCESSDO_LICITACAO
    # CtVigenciaInicio ------------- DATA_INICIO
    # CtVigenciaTermino2 ----------- DATA_FINAL
    # Tempo ------------------------ DURACAO_MESES
    # OrNome ----------------------- ORGAO
    # Credor ----------------------- FORNECEDOR
    # ObNome ----------------------- OBJETIVO
    # CtValorTotal ----------------- VALOR_ORIGEM
    # Textbox7 --------------------- X
    # CtValorTotal1 ---------------- VALOR_TOTAL
    #

    colunas = {
               'CtNumero':'NUMERO_CONTRATO',
               'NU_ProcessoLicitacao': 'NUMERO_PROCESSO_LICITACAO', 
               'CtVigenciaInicio': 'DATA_INICIO',
               'CtVigenciaTermino2': 'DATA_FINAL',
               'Tempo':'DURACAO_MESES',
               'OrNome':'ORGAO',
               'Credor':'FORNECEDOR',
               'ObNome':'OBJETIVO',
               'CtValorTotal':'VALOR_ORIGEM',
               'CtValorTotal1':'VALOR_TOTAL,'
               'CNPJ'
              }

    if(int(ANO_LICITACAO) >= 2021 ):
        gov_1 = pd.read_csv('./base_de_dados/listaContratos_gov_'+ANO_LICITACAO+'.csv', delimiter=',', encoding = 'UTF-8')
        df_gov_cont = gov_1
    else:
        gov_1 = pd.read_csv('./base_de_dados/listaContratos_gov_'+ANO_LICITACAO+'.csv', delimiter=',', encoding = 'UTF-8')
        gov_2 = pd.read_csv('./base_de_dados/listaContratos_gov_'+str((int(ANO_LICITACAO)+1))+'.csv', delimiter=',', encoding = 'UTF-8')
        df_gov_cont = pd.concat([gov_1,gov_2])
        
    df_gov_cont.drop('Textbox7', axis=1, inplace=True)
    df_gov = df_gov_cont.rename(colunas, axis = 1)
    
    df_gov.to_csv('./arquivos_gerados/df_contratos_governo_pb.csv')


def df_licitacao_gov_sagres(ANO_LICITACAO):
    
    #------------------------------------------------------------------------------
    # US3713
    # AUTOR: MATIAS RIBEIRO
    #
    # REQUISITOS:
    # Internet e diponibilidade do site do TCE
    # 
    # COLUNAS:
    # protocolo_licitacao: campo texto com o número de protocolo no sistema de tramitação do TCE, o Tramita;
    # numero_licitacao: campo texto;
    # nome_modalidade_licitacao: campo texto;
    # nome_municipio: campo texto;
    # cd_ugestora: compo numérico com o código do sistema Sagres para o jurisdicionado;
    # jurisdicionado_id: campo numérico com o código do jurisdicionado no sistema Tramita;
    # nome_jurisdicionado: campo texto;
    # nome_tipo_jurisdicionado: campo texto, o tipo pode ser "Prefeitura", "Órgão", "Poder", etc.;
    # nome_tipo_administracao_jurisdicionado: campo texto para o tipo, que pode ser "Direta" ou "Indireta"
    # nome_esfera_jurisdicionado: campo texto, assumindo os valores "Municipal" ou "Estadual";
    # objeto_licitacao: campo texto com a descrição da licitação;
    # valor_estimado_licitacao: campo monetário, sendo o ponto o separador decimal;
    # valor_licitado_licitacao: campo monetário, sendo o ponto o separador decimal;
    # data_homologacao_licitacao: campo data, com o formato DD/MM/YYYY;
    # ano_homologacao_licitacao: campo numérico
    # situacao_fracassada_licitacao: campo texto, assumindo os valores "Não" ou "Sim";
    # nome_proponente: campo texto;
    # cpf_cnpj_proponente: campo texto;
    # valor_proposta: campo monetário, sendo o ponto o separador decimal;
    # situacao_proposta: campo texto, assumindo os valores "Vencedora" ou "Perdedora";
    # nome_estagio_processual_licitacao: campo texto;
    # nome_setor_atual_licitacao: campo texto com o nome do setor do Tribunal onde se encontra do processo/documento;
    # url: URL para consulta do processo/documento;
    #------------------------------------------------------------------------------

    url = "https://dados.tce.pb.gov.br/TCE-PB-Portal-Gestor-Licitacoes_Propostas.txt.gz"
    resp = urlopen(url)

    with gzip.open(resp, 'rb') as entrada:
        with open('licitacao_sagres.txt', 'wb') as saida:
            shutil.copyfileobj(entrada, saida)

    df = pd.read_csv('licitacao_sagres.txt', delimiter='|',  dtype='unicode')

    df_licitacao_gov = df.loc[ 
                    (df['nome_tipo_jurisdicionado']=='Secretaria de Estado')
                  & (df['nome_tipo_administracao_jurisdicionado']=='Direta')
                  & (df['nome_esfera_jurisdicionado']=='Estadual')
                  & (df['ano_homologacao_licitacao']==ANO_LICITACAO)
                  & (df['situacao_proposta']=='Vencedora')
               ]
    df_licitacao_gov = df_licitacao_gov.assign(FORNECEDORES='')


    df_licitacao_gov.to_csv('./arquivos_gerados/licitacao_sagres_por_ano.csv')


def carga_csv_no_mongoDB(  my_df,
                          database_name = 'licitacoesecontratos',
                          collection_name = 'contratosgov',
                          server = 'localhost',
                          mongodb_port = 27017,):
    
    #------------------------------------------------------------------------------
    # US3710, US3711, US3713, US3714
    # AUTOR: MATIAS RIBEIRO
    #
    # REQUISITOS:
    # Banco de dados MongoDB instalado com um dada base 'licitacoesecontratos' sem senha
    # configurações deste banco de dados são para testes
    #------------------------------------------------------------------------------
    
    client = MongoClient('localhost',int(mongodb_port))
    db = client[database_name]
    collection = db[collection_name]

    collection.delete_many({}) 
    my_list = my_df.to_dict('records')
    collection.insert_many(my_list) 
    
    client.close()


def licitacao_com_numero_processo_adm(ANO_LICITACAO):

    df_sagres = pd.read_csv('licitacao_sagres_por_ano.csv', delimiter=',',  dtype='unicode')
    df_url = df_sagres[['url']].drop_duplicates()
    df_url = df_url.assign(NUMERO_PROCESSO_LICITACAO='')
    df_url = df_url.assign(CONTRATOS='')

    for r,p in df_url.iterrows():

        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        driver = webdriver.Chrome('chromedriver', options=options)

        driver.get(str(p.url.strip()))
        time.sleep(8)
        driver.switch_to.frame(0)
        elemento = driver.find_element_by_id("body:mainForm:numeroProcessoAdministrativo").get_attribute('innerHTML')
        df_url.loc[r, 'NUMERO_PROCESSO_LICITACAO'] = elemento
        driver.quit()              

    df_url.to_csv('./arquivos_gerados/licitacao_com_numero_processo_adm.csv') # retorna um DataFrame com url e Processo administrativo


tempo_inicio = time.time()

ANO_DA_LICITACAO = '2021'

df_licitacao_gov_sagres(ANO_DA_LICITACAO)
df_sagres = pd.read_csv('./arquivos_gerados/licitacao_sagres_por_ano.csv', delimiter=',',  dtype='unicode')

lista = []
data = {}
verificador = False
for i,f in df_sagres.iterrows():

    if(f.numero_licitacao in data):
        lista.append(f.cpf_cnpj_proponente)
        verificador == True
    elif (verificador == False):
        verificador = True
        lista.append(f.cpf_cnpj_proponente)
    else:
        lista = []
        lista.append(f.cpf_cnpj_proponente)
    data[f.numero_licitacao] = lista
    df_sagres.at[i,'FORNECEDORES'] = lista

#licitacao_com_numero_processo_adm(ANO_DA_LICITACAO)
df_juncao_gov = pd.read_csv('./arquivos_gerados/licitacao_com_numero_processo_adm.csv', delimiter=',',  dtype='unicode')

#junção por 'url'
juncao_2018 = pd.merge(df_sagres, df_juncao_gov, on='url', how="left")
df_juncao_gov['NUMERO_PROCESSO_LICITACAO'].str.replace('.','',regex=False)
df_sagres_numero_processos = juncao_2018.loc[:, ~juncao_2018.columns.str.contains('^Unnamed')]

# gov
df_contratos_governo_pb(ANO_DA_LICITACAO)
df_gov = pd.read_csv('./arquivos_gerados/df_contratos_governo_pb.csv', delimiter=',',  dtype='unicode')
df_gov['NUMERO_PROCESSO_LICITACAO'].str.replace('.','',regex=False)
df_gov = df_gov.loc[:, ~df_gov.columns.str.contains('^Unnamed')]

# Associar os contratos com as licitações
lista_contratos = []
for i,s in df_sagres_numero_processos.iterrows():
    for j, g in df_gov.iterrows():
              
        if(s.NUMERO_PROCESSO_LICITACAO == g.NUMERO_PROCESSO_LICITACAO):
            
            df_gov.at[j,'CNPJ'] = df_gov.loc[j,'FORNECEDOR'][0:18]
            df_gov.at[j,'FORNECEDOR'] = df_gov.loc[j,'FORNECEDOR'][21:]
            lista_contratos.append(df_gov.loc[j].to_dict())
            
    df_sagres_numero_processos.at[i,'CONTRATOS'] = lista_contratos
    lista_contratos = []

df_sagres_numero_processos = df_sagres_numero_processos.drop(columns=['cd_ugestora',
                                                                     'nome_jurisdicionado',
                                                                     'nome_tipo_jurisdicionado',
                                                                     'nome_tipo_administracao_jurisdicionado',
                                                                     'nome_esfera_jurisdicionado',
                                                                     'situacao_fracassada_licitacao',
                                                                     'nome_proponente',
                                                                     'cpf_cnpj_proponente',
                                                                     'valor_proposta',
                                                                     'situacao_proposta'])


df_sagres_numero_processos.to_csv('doc_retirar_licitacao_sem_contrato.csv')    
df_sagres_numero_processos = df_sagres_numero_processos.drop_duplicates(subset='numero_licitacao', keep='first')

tempoExec = time.time() - tempo_inicio
print("Tempo de execução: {} segundos".format(tempoExec))

carga_csv_no_mongoDB(df_sagres_numero_processos)
