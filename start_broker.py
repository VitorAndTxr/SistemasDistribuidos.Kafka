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
        leader_uri = ns.lookup("Leader-Epoca1")
        # Existe um líder, registrar-se com ele
        # Inicialmente, criar um votante
        brokerBase = BrokerBase()
        registeredBroker = brokerBase.start()

        if registeredBroker['role'] == 'Voter':
            broker = VoterBroker(registeredBroker['broker_id'])
            broker.start()
        else:
            broker = ObserverBroker(registeredBroker['broker_id'])
            broker.start()
            broker = VoterBroker(registeredBroker['broker_id'])
            broker.start()
    except Pyro4.errors.NamingError:
        # Não existe líder, tornar-se o líder
        broker = LeaderBroker()
        broker.start()

if __name__ == "__main__":
    main()