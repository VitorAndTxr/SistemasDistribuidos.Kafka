import Pyro4

def main():
    ns = Pyro4.locateNS()
    leader_uri = ns.lookup("Leader-Epoca1")
    leader = Pyro4.Proxy(leader_uri)
    print("Publisher is running. Type messages to send. Type 'exit' to quit.")
    while True:
        message = input("Enter message: ")
        if message.lower() == 'exit':
            break
        response = leader.publish(message)
        print(f"Leader response: {response}")

if __name__ == "__main__":
    main()
