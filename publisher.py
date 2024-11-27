import Pyro4

def main():
    ns = Pyro4.locateNS()
    try:
        leader_uri = ns.lookup("Leader-Epoca1")
    except Pyro4.errors.NamingError:
        print("Nenhum líder encontrado. Não é possível publicar dados.")
        return
    leader = Pyro4.Proxy(leader_uri)

    print("Publicador iniciado. Digite mensagens para enviar ao líder.")
    while True:
        data = input("Mensagem: ")
        if data.lower() == 'sair':
            break
        leader.receive_publication(data)

if __name__ == "__main__":
    main()