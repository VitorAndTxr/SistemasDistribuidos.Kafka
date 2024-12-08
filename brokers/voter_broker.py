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
        print(f"Solicitando novo fetch de dados.")
        threading.Thread(target=self.fetch_and_replicate, daemon=True).start()

    @Pyro4.expose
    def consolidate_log(self, offset):
        self.commit_log_by_offset(offset)

    def fetch_and_replicate(self):
        fetch_epoch = self.get_last_epoch()
        fetch_offset = self.get_offset()
        print(f"Fetching data from epoch {fetch_epoch}, offset {fetch_offset}")
        response = self.leader.fetch_data(fetch_epoch, fetch_offset)
        if 'error' in response:
            print(f"Inconsistência detectada: {response['error']}")
            max_epoch = response.get('max_epoch', fetch_epoch)
            max_offset = response.get('max_offset', fetch_offset)
            # Truncar log
            with self.lock:
                print(f"Truncando log até offset {max_offset} e epoch {max_epoch}")
                self.log = self.log[:max_offset + 1]
                self.epoch = max_epoch
            # Tentar novamente
            print("Tentando buscar e replicar novamente após truncar log")
            self.fetch_and_replicate()
        else:
            committed_data = response['commited']
            uncommitted_data = response['uncommited']
            print(f"Dados recebidos: {response}")
            # Enviar ACKs
            if len(committed_data) > 0:
                for entry in committed_data:
                    self.update_log(entry)
                    self.leader.receive_ack(self.broker_id, entry['offset'])
                    print(f"ACK enviado para offset {entry['offset']}")

            if len(uncommitted_data) > 0:
                for entry in uncommitted_data:
                    self.update_uncommited_log(entry)
                    self.leader.receive_ack(self.broker_id, entry['offset'])
                    print(f"ACK enviado para offset {entry['offset']}")

            print(f"Votante {self.broker_id} replicou dados até offset {len(self.log) + len(self.uncommited_log) - 1}")

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
