import Pyro4
from Pyro4.errors import NamingError

def main():
    daemon = Pyro4.Daemon()
    ns = Pyro4.locateNS()
    if not verify_if_leader_is_registered(ns):
        

def verify_if_leader_is_registered(ns):
    try:
        uri = ns.lookup("Leader-Epoca1")
        return True
    except NamingError:
        return False