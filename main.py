from neo4j import GraphDatabase
import pandas as pd 
from neo4j import GraphDatabase

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
    
    #Cadastrar orgão
    def add_orgao(self, nome, codigo):
        with self._driver.session() as session:
            session.run("CREATE (a:Orgao {nome: $nome, codigo: $codigo})", nome=nome, codigo=codigo)
            ()

    #Cadastrar pessoa
    def add_licitacao(self, numero, objeto, situacao, valor, data):
        with self._driver.session() as session:
            session.run("CREATE (:Licitacao {numero: $numero, objeto: $objeto, situacao:$situacao, valor:$valor, data:$data})", 
            numero=numero, objeto=objeto, situacao=situacao, valor=valor, data=data)

    #Cadastrar contrato
    def add_contrato(self, numero, objeto, valor, data):
        with self._driver.session() as session:
            session.run("CREATE (:Licitacao {numero: $numero, objeto: $objeto, valor:$valor, data:$data})", 
            numero=numero, objeto=objeto, valor=valor, data=data)

    #Cadastrar empresa
    def add_empresa(self, nome_social, nome_fantasia, cnpj):
        with self._driver.session() as session:
            session.run("CREATE (:Licitacao {nome_social: $nome_social, nome_fantasia:$nome_fantasia, cnpj:$cnpj})", 
            nome_social=nome_social, nome_fantasia=nome_fantasia, cnpj=cnpj)


driver = new4jDriver('bolt://localhost:7687',"neo4j", "123456")
'''
cnpj_file = pd.read_csv('201908_CNPJ.csv', sep=';', encoding='cp860')
for index, row in cnpj_file.iterrows():
    driver.add_empresa(row['RAZAOSOCIAL'], row['NOMEFANTASIA'],row['CNPJ'])


cadastro_file = pd.read_csv('201908_Cadastro.csv', sep=';', encoding='cp860')
for index, row in cadastro_file.iterrows():
    driver.add_pessoa(row['NOME'], row['CPF'])

licitacao_file = pd.read_csv('201908_Licitaá∆o.csv', sep=';', encoding='cp860')
for index, row in licitacao_file.iterrows():
    driver.add_licitacao(row['Número Licitação'], row['Objeto'], row['Situação Licitação'], row['Valor Licitação'], row['Data Resultado Compra'])

for index, row in licitacao_file.iterrows():
    driver.add_orgao(row['Nome Órgão'], row['Código Órgão'])

compras_file = pd.read_csv('201908_Compras.csv', sep=';', encoding='cp860')
for index, row in licitacao_file.iterrows():
    driver.add_contrato(row['Número do Contrato'], row['Objeto'], row['Valor Final Compra'], row['Data Início Vigência'])

'''

cnpj_file = pd.read_csv('201908_CNPJ.csv', sep=';', encoding='cp860')
for index, row in cnpj_file.head().iterrows():
    driver.add_empresa(row['RAZAOSOCIAL'], row['NOMEFANTASIA'],row['CNPJ'])


cadastro_file = pd.read_csv('201908_Cadastro.csv', sep=';', encoding='cp860')
for index, row in cadastro_file.head().iterrows():
    driver.add_pessoa(row['NOME'], row['CPF'])

licitacao_file = pd.read_csv('201908_Licitaá∆o.csv', sep=';', encoding='cp860')
for index, row in licitacao_file.head().iterrows():
    driver.add_licitacao(row['Número Licitação'], row['Objeto'], row['Situação Licitação'], row['Valor Licitação'], row['Data Resultado Compra'])

for index, row in licitacao_file.iterrows():
    driver.add_orgao(row['Nome Órgão'], row['Código Órgão'])

compras_file = pd.read_csv('201908_Compras.csv', sep=';', encoding='cp860')
for index, row in licitacao_file.head().iterrows():
    driver.add_contrato(row['Número do Contrato'], row['Objeto'], row['Valor Final Compra'], row['Data Início Vigência'])





