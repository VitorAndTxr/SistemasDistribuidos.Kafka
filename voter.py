import time
import uuid
import Pyro4
import threading

@Pyro4.expose
class VoterBroker:
    def __init__(self, broker_id):
        self.broker_id = broker_id
        self.state = 'Voter'
        self.log = []

        self.last_epoch = 1  # Initialize with epoch 1
        self.lock = threading.Lock()
        self.leader = None  # Will be set after registration
        self.heartbeat_interval = 3  # seconds
        self.brokers_info = {}  # Updated when leader notifies

    
    def new_data_available(self):
        # Leader has notified that new data is available
        threading.Thread(target=self.fetch_and_replicate).start()
    
    def fetch_and_replicate(self):
        with self.lock:
            fetch_epoch = self.log[-1]['epoch'] if self.log else self.last_epoch
            fetch_offset = len(self.log)
            response = self.leader.fetch_data(self.broker_id, fetch_epoch, fetch_offset)
        
        if 'error' in response:
            print(f"Broker {self.broker_id}: Fetch error - {response['error']}")
            max_epoch = response.get('max_epoch', fetch_epoch)
            max_offset = response.get('max_offset', fetch_offset)
            # Truncate log to max_offset
            with self.lock:
                self.log = self.log[:max_offset + 1]
                self.last_epoch = max_epoch
            # Retry fetch with updated epoch and offset
            self.fetch_and_replicate()
        else:
            data = response['data']
            with self.lock:
                self.log.extend(data)
                if data:
                    self.last_epoch = data[-1]['epoch']
            # Send acknowledgment for each received entry
            for entry in data:
                self.leader.receive_ack(self.broker_id, entry['offset'])
            print(f"Broker {self.broker_id}: Replicated entries up to offset {self.log[-1]['offset']}")
    
    def start(self):
        daemon = Pyro4.Daemon()
        uri = daemon.register(self)
        
        # Locate leader
        ns = Pyro4.locateNS()
        leader_uri = ns.lookup("Leader-Epoca1")
        self.leader = Pyro4.Proxy(leader_uri)

        # Inicia o thread de heartbeat aqui
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

        # Register with leader
        self.leader.register_broker(self.broker_id, uri.asString(), 'Voter')
        print(f"Voter broker {self.broker_id} is running.")
        daemon.requestLoop()

    def update_quorum(self, brokers_info):
        with self.lock:
            # Update local brokers information
            self.brokers_info = brokers_info
            print(f"{self.broker_id}: Updated quorum information.")
    
    def send_heartbeats(self):
        while True:
            time.sleep(self.heartbeat_interval)
            try:
                self.leader.heartbeat(self.broker_id)
            except Exception as e:
                print(f"Failed to send heartbeat: {e}")
    
def main():
    import sys
    if len(sys.argv) > 2:
        print("Usage: python voter.py or voter.py <broker_id>")
        exit(1)
    
    broker_id = uuid.uuid4()
    
    if len(sys.argv) == 2:
        broker_id = sys.argv[1]  # Use provided broker_id

    voter = VoterBroker(broker_id)
    voter.start()

if __name__ == "__main__":
    main()
