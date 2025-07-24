from time import sleep

def primeira_atividade():
    print("Primeira atividade rodou com sucesso")
    sleep(2)


def segunda_atividade():
    print("Segunda atividade rodou com sucesso")
    sleep(2)

def terceira_atividade():
    print("Terceira atividade rodou com sucesso")
    sleep(2)

def pipeline():
    primeira_atividade()
    segunda_atividade()
    terceira_atividade()


pipeline()