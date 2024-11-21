import Pyro4

def main():
    ns = Pyro4.locateNS()
    # For simplicity, consumer connects to the leader
    leader_uri = ns.lookup("Leader-Epoca1")
    broker = Pyro4.Proxy(leader_uri)
    start_offset = 0
    print("Consumer is running. Type 'fetch' to get new messages. Type 'exit' to quit.")
    while True:
        cmd = input("Enter command: ")
        if cmd.lower() == 'exit':
            break
        elif cmd.lower() == 'fetch':
            messages = broker.consume(start_offset)
            if messages:
                for msg in messages:
                    print(f"Offset {msg['offset']}: {msg['message']} (Epoch {msg['epoch']})")
                start_offset = messages[-1]['offset'] + 1
            else:
                print("No new messages.")
        else:
            print("Unknown command.")

if __name__ == "__main__":
    main()
