# Branch com script em python para integração dos dados
# 23/04/2021
# Criada por: Matias Ribeiro

import pandas as pd
import numpy as np
import collections
import gzip
import shutil
import pymongo
import time
from urllib.request import urlopen
from pymongo import MongoClient

def df_constratos_governo_pb():
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
               'NU_ProcessoLicitacao': 'NUMERO_PROCESSDO_LICITACAO', 
               'CtVigenciaInicio': 'DATA_INICIO',
               'CtVigenciaTermino2': 'DATA_FINAL',
               'Tempo':'DURACAO_MESES',
               'OrNome':'ORGAO',
               'Credor':'FORNECEDOR',
               'ObNome':'OBJETIVO',
               'CtValorTotal':'VALOR_ORIGEM',
               'CtValorTotal1':'VALOR_TOTAL'
              }

    gov_2018 = pd.read_csv('./base_de_dados/listaContratos_gov_2018.csv', delimiter=',', encoding = 'UTF-8')
    gov_2019 = pd.read_csv('./base_de_dados/listaContratos_gov_2019.csv', delimiter=',', encoding = 'UTF-8')
    gov_2020 = pd.read_csv('./base_de_dados/listaContratos_gov_2020.csv', delimiter=',', encoding = 'UTF-8')
    gov_2021 = pd.read_csv('./base_de_dados/listaContratos_gov_2021.csv', delimiter=',', encoding = 'UTF-8')

    df_gov_cont = pd.concat([gov_2018,gov_2019,gov_2020,gov_2021])
    df_gov_cont.drop('Textbox7', axis=1, inplace=True)
    df_gov = df_gov_cont.rename(colunas, axis = 1)
    
    return df_gov


def df_licitacao_gov_sagres():
    
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

    df_licitacao_gov = df.loc[ (df['cd_ugestora']=='0')
                     & ( (df['ano_homologacao_licitacao']=='2018') | (df['ano_homologacao_licitacao']=='2019')
                       | (df['ano_homologacao_licitacao']=='2020') | (df['ano_homologacao_licitacao']=='2021') )
                     & (df['nome_jurisdicionado'].str.contains('Secretaria de Estado da Administração'))]

    return df_licitacao_gov


def carga_csv_no_mongoDB(  my_df,
                          database_name = 'licitacoesecontratos',
                          collection_name = 'contratosgov',
                          server = 'localhost',
                          mongodb_port = 27017,
                          chunk_size = 100):
    
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

    
