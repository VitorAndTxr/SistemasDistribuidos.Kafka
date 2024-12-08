# consumer.py

import Pyro4

def main():
    ns = Pyro4.locateNS()
    leader_uri = None
    try:
        leader_uri = ns.lookup("Leader-Epoca1")
    except Pyro4.errors.NamingError:
        print("Nenhum líder encontrado. Não é possível consumir dados.")
        return
    leader = Pyro4.Proxy(leader_uri)

    # Consumidor irá buscar dados a partir do offset 0
    offset = 0
    print("Consumidor iniciado.")
    while True:
        input("Pressione Enter para buscar novas mensagens...")
        epoch = leader.get_epoch()
        response = leader.get_data(epoch, offset)
        if 'error' in response:
            print(f"Erro ao buscar dados: {response['error']}")
        else:
            data = response['data']
            if data:
                for entry in data:
                    print(f"Offset {entry['offset']}: {entry['data']}")
                offset = data[-1]['offset'] + 1
            else:
                print("Sem novas mensagens.")

if __name__ == "__main__":
    main()
