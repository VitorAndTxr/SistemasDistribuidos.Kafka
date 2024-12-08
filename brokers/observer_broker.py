# brokers/observer_broker.py

import time
from .broker_base import BrokerBase
import Pyro4
import threading

class ObserverBroker(BrokerBase):
    def __init__(self, broker_id):
        super().__init__()
        self.state = 'Observer'
        self.broker_id = broker_id

    def start(self):
        self.start_daemon()
        self.register_leader()
        if self.leader is None:
            print("Nenhum líder encontrado. Não é possível Votante")
            return
        # Registrar-se com o líder
        self.leader.register_broker(self.broker_id, self.uri)
        print(f"Registrado com o líder como {self.state}.")

        self.request_loop()

    def check_leader(self):
        try:
            return self.ns.lookup("Leader-Epoca1")
        except Pyro4.errors.NamingError:
            return None

    @Pyro4.expose
    def handle_new_data(self):
        threading.Thread(target=self.fetch_and_replicate, daemon=True).start()

    def fetch_and_replicate(self):
        fetch_epoch = self.get_last_epoch()
        fetch_offset = self.get_offset()
        response = self.leader.fetch_data(fetch_epoch, fetch_offset)
        if 'error' in response:
            print(f"Inconsistência detectada: {response['error']}")
            max_epoch = response.get('max_epoch', fetch_epoch)
            max_offset = response.get('max_offset', fetch_offset)
            # Truncar log
            with self.lock:
                self.log = self.log[:max_offset + 1]
                self.epoch = max_epoch
            # Tentar novamente
            self.fetch_and_replicate()
        else:
            committed_data = response['commited']
            uncommitted_data = response['uncommited']

            # Enviar ACKs
            
            if(len(committed_data) > 0):
                self.update_log(committed_data)
                for entry in committed_data:
                    self.leader.receive_ack(self.broker_id, entry['offset'])

            if(len(uncommitted_data) > 0):
                self.update_uncommited_log(uncommitted_data)
                for entry in uncommitted_data:
                    self.leader.receive_ack(self.broker_id, entry['offset'])

            print(f"Obser {self.broker_id} replicou dados até offset {len(self.log)+len(self.uncommited_log) - 1}")

    @Pyro4.expose
    def update_role(self, new_role):
        self.state = new_role
        print(f"Broker {self.broker_id} agora é {self.state}.")

        self.daemon.shutdown()

    def send_heartbeats(self):
        while True:
            time.sleep(3)
            try:
                self.leader.heartbeat(self.broker_id)
            except Exception as e:
                print(f"Falha ao enviar heartbeat: {e}")
