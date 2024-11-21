import Pyro4

def main():
    ns = Pyro4.locateNS()  # Localiza o Name Server
    uri = ns.lookup("exemplo.servidor_teste")  # Busca o objeto remoto
    servidor = Pyro4.Proxy(uri)  # Cria um proxy para o objeto remoto
    resposta = servidor.ola("Mundo")
    print(resposta)

if __name__ == "__main__":
    main()