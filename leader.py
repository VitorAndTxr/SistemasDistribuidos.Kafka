import Pyro4
import threading
import time

@Pyro4.expose

class LeaderBroker:   
    def __init__(self):
        self.broker_id = 'Leader'
        self.state = 'Leader'
        self.log = []  # List of log entries
        self.epoch = 1
        self.brokers = {}  # Registered brokers: {broker_id: {'reference': uri, 'state': state}}
        self.quorum_size = 3  # Leader + 2 voters
        self.lock = threading.Lock()  # For thread-safe operations
        self.acknowledgments = {}  # {offset: set(broker_ids)}
        self.heartbeat_timeout = 10  # seconds
        self.last_heartbeat = {}  # {broker_id: last_heartbeat_time}
        # Start a thread to monitor heartbeats
        threading.Thread(target=self.monitor_heartbeats, daemon=True).start()   

    def heartbeat(self, broker_id):
        with self.lock:
            self.last_heartbeat[broker_id] = time.time()

    def monitor_heartbeats(self):
        while True:
            time.sleep(self.heartbeat_timeout / 2)
            with self.lock:
                current_time = time.time()
                failed_voters = []
                for broker_id in list(self.last_heartbeat.keys()):
                    if current_time - self.last_heartbeat[broker_id] > self.heartbeat_timeout:
                        print(f"Voter {broker_id} failed (no heartbeat received).")
                        failed_voters.append(broker_id)
                for broker_id in failed_voters:
                    self.handle_voter_failure(broker_id)

    def register_broker(self, broker_id, broker_uri, state):
        with self.lock:
            self.brokers[broker_id] = {'reference': broker_uri, 'state': state}
            if state == 'Voter':
                self.last_heartbeat[broker_id] = time.time()
        print(f"Broker {broker_id} registered as {state}")

    def notify_voters_about_change(self):
        for broker_id, info in self.brokers.items():
            if info['state'] == 'Voter':
                try:
                    voter = Pyro4.Proxy(info['reference'])
                    voter.update_quorum(self.brokers)
                    print(f"Notified voter {broker_id} about quorum change.")
                except Exception as e:
                    print(f"Failed to notify voter {broker_id}: {e}")


    def promote_observer_to_voter(self):
        # Find an observer to promote
        for broker_id, info in self.brokers.items():
            if info['state'] == 'Observer':
                # Update broker state
                self.brokers[broker_id]['state'] = 'Voter'
                self.last_heartbeat[broker_id] = time.time()
                # Notify the observer
                try:
                    observer = Pyro4.Proxy(info['reference'])
                    observer.promote_to_voter()
                    print(f"Promoted observer {broker_id} to voter.")
                    # Notify existing voters
                    self.notify_voters_about_change()
                    return
                except Exception as e:
                    print(f"Failed to promote observer {broker_id}: {e}")
        print("No observers available to promote.")


    def handle_voter_failure(self, broker_id):
        # Remove the failed voter
        del self.brokers[broker_id]
        del self.last_heartbeat[broker_id]
        print(f"Removed voter {broker_id} due to failure.")
        # Check if quorum is still met
        active_voters = [bid for bid, info in self.brokers.items() if info['state'] == 'Voter']
        quorum_size = len(active_voters) + 1  # +1 for leader
        required_quorum = (self.quorum_size // 2) + 1
        if quorum_size < required_quorum:
            print("Quorum lost. Promoting an observer to voter.")
            self.promote_observer_to_voter()

        
    def publish(self, message):
        with self.lock:
            offset = len(self.log)
            log_entry = {
                'offset': offset,
                'message': message,
                'epoch': self.epoch,
                'committed': False
            }
            self.log.append(log_entry)
            self.acknowledgments[offset] = set()
            print(f"Received message: '{message}', offset: {offset}, epoch: {self.epoch}")
        
        # Notify voters to fetch data
        for broker_id, info in self.brokers.items():
            if info['state'] == 'Voter':
                try:
                    voter = Pyro4.Proxy(info['reference'])
                    threading.Thread(target=voter.new_data_available).start()
                except Exception as e:
                    print(f"Error notifying broker {broker_id}: {e}")
        
        # Wait for commit (in a real system, you might not block here)
        return "Message received and replication initiated."
    
    def fetch_data(self, broker_id, fetch_epoch, fetch_offset):
        with self.lock:
            # Check if fetch_epoch and fetch_offset are consistent
            if fetch_offset >= len(self.log):
                # Voter is ahead, which should not happen
                return {'error': 'Offset out of range', 'max_epoch': self.epoch, 'max_offset': len(self.log) - 1}
            
            leader_entry = self.log[fetch_offset]
            if leader_entry['epoch'] != fetch_epoch:
                # Epoch mismatch
                # Find the highest epoch before the requested epoch
                max_epoch = leader_entry['epoch']
                max_offset = fetch_offset - 1
                return {'error': 'Epoch mismatch', 'max_epoch': max_epoch, 'max_offset': max_offset}
            
            # Fetch data from fetch_offset onwards
            data = self.log[fetch_offset:]
            return {'data': data}
    
    def receive_ack(self, broker_id, offset):
        with self.lock:
            self.acknowledgments[offset].add(broker_id)
            print(f"Received ack from {broker_id} for offset {offset}")
            # Check if quorum is achieved
            if len(self.acknowledgments[offset]) + 1 >= (self.quorum_size // 2) + 1:  # +1 for leader
                if not self.log[offset]['committed']:
                    self.log[offset]['committed'] = True
                    print(f"Offset {offset} committed.")
    
    # Additional methods for consumer requests
    def consume(self, start_offset):
        with self.lock:
            if start_offset < len(self.log):
                return self.log[start_offset:]
            else:
                return []

def main():
    leader = LeaderBroker()
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    uri = daemon.register(leader)
    ns.register("Leader-Epoca1", uri)
    print("Leader broker is running.")
    daemon.requestLoop()

if __name__ == "__main__":
    main()
