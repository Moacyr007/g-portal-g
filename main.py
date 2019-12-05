# -*- coding: utf-8 -*-

from neo4j import GraphDatabase
import pandas as pd 
import csv


class new4jDriver(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    #Cadastrar pessoa
    def add_pessoa(self, nome, cpf):
        with self._driver.session() as session:
            session.run("CREATE (a:Pessoa {nome: $nome, cpf: $cpf})", nome=nome, cpf=cpf)
    
    def gerar_script_add_pessoa(self, nome, cpf):
        return ("CREATE (:Pessoa {nome:"+str(nome)+", cpf:"+str(cpf)+"})")
    
    #Cadastrar orgao
    def add_orgao(self, nome, codigo):
        with self._driver.session() as session:
            session.run("CREATE (a:Orgao {nome: $nome, codigo: $codigo})", nome=nome, codigo=codigo)
    def gerar_script_add_orgao(self, nome, codigo):
        return ("CREATE (:Orgao {nome:"+str(nome)+", codigo:"+str(codigo)+"})")
            
    #Cadastrar pessoa
    def add_licitacao(self, numero, objeto, situacao, valor, data):
        with self._driver.session() as session:
            session.run("CREATE (:Licitacao {numero: $numero, objeto: $objeto, situacao:$situacao, valor:$valor, data:$data})", 
            numero=numero, objeto=objeto, situacao=situacao, valor=valor, data=data)
    
    def gerar_script_add_licitacao(self, numero, objeto, situacao, valor, data):
        return ("CREATE (:Licitacao {numero:"+str(numero)+", objeto:"+str(objeto)+", situacao:"+str(situacao)+", valor:"+str(valor)+", data:"+str(data)+"})")

    #Cadastrar contrato
    def add_contrato(self, numero, objeto, valor, data):
        with self._driver.session() as session:
            session.run("CREATE (:Contrato {numero: $numero, objeto: $objeto, valor:$valor, data:$data})", 
            numero=numero, objeto=objeto, valor=valor, data=data)

    def gerar_script_add_contrato(self, numero, objeto, valor, data):
        return ("CREATE (:Contrato {numero:"+str(numero)+", objeto:"+str(objeto)+", valor:"+str(valor)+", data:"+str(data)+"})")



    #Cadastrar empresa
    def gerar_script_add_empresa(self, nome_social, nome_fantasia, cnpj):
        return ("CREATE (:Empresa {nome_social:"+str(nome_social)+", nome_fantasia:"+str(nome_fantasia)+", cnpj:"+str(cnpj)+"})")


    def rodar_no_neo4j(self, script):
        with self._driver.session() as session:
            session.run(script)




    def add_socio(self, cnpj_empresa, cpj_cnpj_socio, tipo):
        with self._driver.session() as session:
            session.run("MATCH (e:Empresa), (es:Empresa) where e.cnpj = $cnpj_empresa and es.cnpj=$cpj_cnpj_socio  CREATE (es)-[:SOCIO {tipo: $tipo}]->(e)",
            cnpj_empresa=cnpj_empresa, cpj_cnpj_socio=cpj_cnpj_socio, tipo=tipo)
            
            session.run("MATCH (e:Empresa), (ps:Pessoa) where e.cnpj = $cnpj_empresa and ps.cnpj=$cpj_cnpj_socio  CREATE (ps)-[:SOCIO {tipo: $tipo}]->(e)",
            cnpj_empresa=cnpj_empresa, cpj_cnpj_socio=cpj_cnpj_socio, tipo=tipo)

driver = new4jDriver('bolt://localhost:7687',"neo4j", "123456")


#Inserir os nós vvvvvv____________________________________________________________________________________________________________________________

cnpj_df = pd.read_csv('201908_CNPJ.csv', sep=';', encoding='latin1')
script = ""
count = 0
for index, row in cnpj_df.iterrows():
    if (count == 1000):
        driver.rodar_no_neo4j(script)
        script = ""
        count = 0
    script = script + driver.gerar_script_add_empresa(row['RAZAOSOCIAL'], row['NOMEFANTASIA'],row['CNPJ'])
    count += count

driver.rodar_no_neo4j(script)
'''
cadastro_df = pd.read_csv('201908_Cadastro.csv', sep=';', encoding='latin1')
for index, row in cadastro_df.head(50).iterrows():
    driver.add_pessoa(row['NOME'], row['CPF'])

licitacao_df = pd.read_csv('201908_Licitaá∆o.csv', sep=';', encoding='latin1', error_bad_lines=False)
for index, row in licitacao_df.head(50).iterrows():
    driver.add_licitacao(row['Número Licitação'], row['Objeto'], row['Situação Licitação'], row['Valor Licitação'], row['Data Resultado Compra'])

#Remover Orgãos duplicados
licitacao_df.sort_values("Código Órgão", inplace = True)
licitacao_df.drop_duplicates(subset ="Código Órgão", keep = 'first', inplace = True) # dropping ALL duplicte values

for index, row in licitacao_df.head(50).iterrows():
    driver.add_orgao(row['Nome Órgão'], row['Código Órgão'])


compras_df = pd.read_csv('201908_Compras.csv', sep=';', encoding='latin1')
for index, row in compras_df.head(50).iterrows():
    driver.add_contrato(row['Número do Contrato'], row['Objeto'], row['Valor Final Compra'], row['Data Início Vigência'])
'''
#Inserir os nós ^^^^^^____________________________________________________________________________________________________________________________

#Inserir as relações vvvvvv____________________________________________________________________________________________________________________________
socios_df = pd.read_csv('201908_Socios.csv', sep=';', encoding='latin1', error_bad_lines=False)
for index, row in socios_df.head(50).iterrows():
    driver.add_socio(row['CNPJ'], row['CPF-CNPJ'], row['Tipo'])




#Inserir as relações ^^^^^^____________________________________________________________________________________________________________________________