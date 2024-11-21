import Pyro4

@Pyro4.expose
class ServidorTeste:
    def ola(self, nome):
        return f"Olá, {nome}!"

def main():
    daemon = Pyro4.Daemon()  # Cria um daemon PyRO
    ns = Pyro4.locateNS()    # Localiza o Name Server
    uri = daemon.register(ServidorTeste)  # Registra o objeto
    ns.register("exemplo.servidor_teste", uri)  # Registra no Name Server
    print("Servidor de teste pronto.")
    daemon.requestLoop()  # Entra no loop de atendimento

if __name__ == "__main__":
    main()
