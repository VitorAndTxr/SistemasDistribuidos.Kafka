# brokers/broker_base.py

import Pyro4
import threading
import time
import uuid

@Pyro4.expose
class BrokerBase:
    def __init__(self):
        self.broker_id = str(uuid.uuid4())
        self.state = None  # Será definido como 'Leader', 'Voter' ou 'Observer'
        self.log = []
        self.leader = None
        self.daemon = None
        self.ns = None
        self.lock = threading.Lock()
        self.epoch = 1  # Época inicial
        self.offset = 0  # Offset inicial

    def start_daemon(self):
        self.daemon = Pyro4.Daemon()
        self.uri = self.daemon.register(self)
        self.ns = Pyro4.locateNS()

    def request_loop(self):
        print(f"{self.state} {self.broker_id} está em execução.")
        self.daemon.requestLoop()

    def update_log(self, entries):
        with self.lock:
            self.log.extend(entries)
            self.offset = len(self.log)

    def get_last_epoch(self):
        with self.lock:
            if self.log:
                return self.log[-1]['epoch']
            else:
                return self.epoch

    def get_offset(self):
        with self.lock:
            return self.offset

    # Métodos que serão sobrescritos ou estendidos pelas subclasses
    def start(self):
        self.start_daemon()
        self.register_leader()
        role = self.leader.register_broker(self.broker_id)

        return {
            "role": role,
            "broker_id": self.broker_id
        }

    def register_leader(self):
        leader_uri = self.ns.lookup("Leader-Epoca1")
        self.leader = Pyro4.Proxy(leader_uri)


    def handle_new_data(self):
        pass  # Implementado em votantes e observadores

    def send_heartbeat(self):
        pass  # Implementado em votantes
