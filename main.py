from neo4j import GraphDatabase
import pandas as pd 
from neo4j import GraphDatabase

#cadastro_file = pd.read_csv('201908_Cadastro.csv', sep=';', encoding='cp860')

#print(cadastro_file.head())

class HelloWorldExample(object):

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

driverTeste = HelloWorldExample('bolt://localhost:7687',"neo4j", "123456")
driverTeste.print_greeting('sad')

