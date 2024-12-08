# brokers/leader_broker.py

from .broker_base import BrokerBase
import Pyro4
import threading
import time

class LeaderBroker(BrokerBase):
    def __init__(self):
        super().__init__()
        self.state = 'Leader'
        self.commited_log = []
        self.voters = {}
        self.observers = {}
        self.quorum_size = 3  # Para tolerar 1 falha, precisamos de 3 votantes (2f+1)
        self.acks = {}  # Rastreamento de confirmações dos votantes


    def start(self):
        self.start_daemon()
        # Registrar-se no serviço de nomes
        try:
            self.ns.register("Leader-Epoca1", self.uri)
            print("Líder registrado como 'Leader-Epoca1' no serviço de nomes.")
        except Pyro4.errors.NamingError:
            print("Já existe um líder registrado. Não é possível iniciar outro líder.")
            return

        # Iniciar thread para monitorar heartbeats dos votantes
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()

        self.request_loop()

    @Pyro4.expose
    def get_epoch(self):
        return self.epoch
    
    @Pyro4.expose
    def register_broker(self, broker_id, broker_uri=None):

        if not broker_uri:

            print(f"Broker {broker_id} solicitou registro.")

            if len(self.voters) < self.quorum_size - 1:
                self.voters[broker_id] = {
                    'uri': '',
                    'last_heartbeat': time.time()
                }
                print(f"Broker {broker_id} registrado como Voter.")
                return 'Voter'
            else:
                self.observers[broker_id] = {
                    'uri': '',
                    'last_heartbeat': time.time()
                }
                print(f"Broker {broker_id} registrado como Observer.")
                return 'Observer'
        else:
            if broker_id in self.voters:
                self.voters[broker_id]['uri'] = broker_uri
                print(f"Broker {broker_id} atualizou URI.")
                return 'Voter'


            elif broker_id in self.observers:
                self.observers[broker_id]['uri'] = broker_uri
                print(f"Broker {broker_id} atualizou URI.")
                return 'Observer'
            else:
                print(f"Broker {broker_id} não encontrado.")

    @Pyro4.expose
    def receive_publication(self, data):
        # Recebe dados do publicador
        entry = {
            'epoch': self.epoch,
            'offset': len(self.log),
            'data': data
        }
        self.log.append(entry)
        self.offset += 1
        print(f"Líder recebeu publicação: {data}")
        # Notificar votantes
        self.notify_voters()

    def notify_voters(self):
        for broker_id, info in self.voters.items():
            try:
                voter = Pyro4.Proxy(info['uri'])
                voter.handle_new_data()
            except Exception as e:
                print(f"Falha ao notificar votante {broker_id}: {e}")

    @Pyro4.expose
    def receive_ack(self, broker_id, offset):
        # Recebe confirmação do votante
        print(f"Líder recebeu ACK de {broker_id} para offset {offset}")

        if offset not in self.acks:
            self.acks[offset] = set()
        self.acks[offset].add(broker_id)
        
        if len(self.acks[offset]) >= (self.quorum_size // 2) + 1:
            print(f"Entrada em offset {offset} consolidada.")

    @Pyro4.expose
    def fetch_data(self, epoch, offset):
        # Enviar dados para o votante
        with self.lock:
            if epoch != self.epoch or offset > self.offset:
                return {
                    'error': 'Inconsistência de epoch ou offset',
                    'max_epoch': self.epoch,
                    'max_offset': self.offset - 1
                }
            else:
                data = self.log[offset:]
                
                return {'data': data}

    @Pyro4.expose
    def heartbeat(self, broker_id):
        # Atualiza o timestamp do heartbeat do votante
        if broker_id in self.voters:
            self.voters[broker_id]['last_heartbeat'] = time.time()
            print(f"Recebido heartbeat do broker Votante {broker_id}")

        elif broker_id in self.observers:
            self.observers[broker_id]['last_heartbeat'] = time.time()
            print(f"Recebido heartbeat do broker Observador{broker_id}")
        else:
            print(f"Recebido heartbeat de broker não registrado {broker_id}")

    def monitor_heartbeats(self):
        while True:
            time.sleep(15)  # Intervalo de monitoramento
            current_time = time.time()
            to_remove = []
            for broker_id, info in self.voters.items():
                if current_time - info['last_heartbeat'] > 30:
                    print(f"Votante {broker_id} falhou.")
                    to_remove.append(broker_id)
            for broker_id in to_remove:
                del self.voters[broker_id]
                self.promote_observer()
                # Notificar os votantes sobre a mudança no quórum

    def promote_observer(self):
        if self.observers:
            broker_id, info = self.observers.popitem()

            self.voters[broker_id] = info
            self.voters[broker_id]['last_heartbeat'] = time.time() +30

            print(f"Observador {broker_id} promovido a Votante.")
            # Notificar o novo votante
            try:
                voter = Pyro4.Proxy(info['uri'])
                voter.update_role('Voter')
            except Exception as e:
                print(f"Falha ao notificar observador {broker_id}: {e}")