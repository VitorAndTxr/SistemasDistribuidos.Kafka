# brokers/voter_broker.py

from .broker_base import BrokerBase
import Pyro4
import threading
import time

class VoterBroker(BrokerBase):
    def __init__(self, broker_id):
        super().__init__()
        self.state = 'Voter'
        self.heartbeat_interval = 3  # Segundos
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

        # Iniciar envio de heartbeats
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

        self.request_loop()

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
            data = response['data']
            self.update_log(data)
            # Enviar ACKs
            for entry in data:
                self.leader.receive_ack(self.broker_id, entry['offset'])
            print(f"Votante {self.broker_id} replicou dados até offset {self.offset - 1}")

    def send_heartbeats(self):
        while True:
            time.sleep(self.heartbeat_interval)
            try:
                self.leader.heartbeat(self.broker_id)
            except Exception as e:
                print(f"Falha ao enviar heartbeat: {e}")

    def update_role(self, new_role):
        self.state = new_role
        print(f"Broker {self.broker_id} agora é {self.state}.")
