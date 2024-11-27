# start_broker.py

import sys
import Pyro4
from brokers.broker_base import BrokerBase
from brokers.leader_broker import LeaderBroker
from brokers.voter_broker import VoterBroker
from brokers.observer_broker import ObserverBroker

def main():
    # Verificar se o líder está registrado
    ns = Pyro4.locateNS()
    try:
        print("Buscando líder...")

        leader_uri = ns.lookup("Leader-Epoca1")
        # Existe um líder, registrar-se com ele
        # Inicialmente, criar um votante
        
        print("Lider encontrado. Iniciando Broker.")

        initialize_broker()

    except Pyro4.errors.NamingError:
        # Não existe líder, tornar-se o líder
        
        print("Nenhum líder encontrado. Iniciando líder.")

        initialize_leader()

def initialize_leader():
    broker = LeaderBroker()
    broker.start()

def initialize_broker():
    brokerBase = BrokerBase()
    registeredBroker = brokerBase.start()

    if registeredBroker['role'] == 'Voter': 
        initiate_voter(registeredBroker)
    else:
        initiate_observer(registeredBroker)

def initiate_observer(registeredBroker):
    broker = ObserverBroker(registeredBroker['broker_id'])
    broker.start()
    initiate_voter(registeredBroker)

def initiate_voter(registeredBroker):
    broker = VoterBroker(registeredBroker['broker_id'])
    broker.start()

if __name__ == "__main__":
    main()
