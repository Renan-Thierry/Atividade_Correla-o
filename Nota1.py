import threading
import queue
from typing import List
import math
from statistics import mean

arquivos_info = {
        "Nat2006us.dat": { "ano": 2006, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2007us.dat": { "ano": 2007, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2008us.dat": { "ano": 2008, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2009usPub.r20131202": { "ano": 2009, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2010PublicUS.r20131202": { "ano": 2010, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2011PublicUS.r20131211": { "ano": 2011, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2012PublicUS.r20131217": { "ano": 2012, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2013PublicUS.r20141016": { "ano": 2013, "idade_pos": slice(88, 90), "peso_pos": slice(462, 466)},
        "Nat2014PublicUS.c20150514.r20151022.txt": { "ano": 2014, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2015PublicUS.c20160517.r20160907.txt": { "ano": 2015, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2016PublicUS.c20170517.r20190620.txt": { "ano": 2016, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2017PublicUS.c20180516.r20180808.txt": { "ano": 2017, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2018PublicUS.c20190509.r20190717.txt": { "ano": 2018, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2019PublicUS.c20200506.r20200915.txt": { "ano": 2019, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2020PublicUS.c20210506.r20210812.txt": { "ano": 2020, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},
        "Nat2021US.txt": { "ano": 2021, "idade_pos": slice(74, 76), "peso_pos": slice(503, 507)},  
        }

fila_arquivo = queue.Queue()  # Cria uma fila vazia

for arquivo in arquivos_info:
    fila_arquivo.put(arquivo)  # Adiciona os arquivos na fila

def Cria_threads(num_threads, fila_arquivo):
    for i in range(num_threads):
        t = threading.Thread(target=processa_Arquivos, args=(fila_arquivo,))
        t.start()

def processa_Arquivos(fila_arquivo):
    while True:
        try:
            arquivo = fila_arquivo.get(block=False)  # Tenta pegar um arquivo da fila
            processa_arquivo(arquivo)  # Processa o arquivo
        except queue.Empty:
            break  # Se a fila estiver vazia sai do loop

def processa_arquivo(arquivo):
    maes_idades = [] # Lista das idades das mães
    bb_pesos = [] # Lista dos pesos médios dos bebês para cada idade de mãe
  
    if arquivo not in arquivos_info:
        raise ValueError("Arquivo não suportado")
        
    info = arquivos_info[arquivo] 
    ano = info["ano"]
    idade_pos = info["idade_pos"]
    peso_pos = info["peso_pos"]
    
    idade_peso_dict = {} # Dicionário para armazenar as idades e pesos separadamente
    with open(arquivo, 'r') as f:
        for linha in f:
            idade = int(linha[idade_pos]) # Extrai a idade da mãe da linha atual
            peso = int(linha[peso_pos]) # Extrai o peso do bebê da linha atual

            if idade not in idade_peso_dict:
                idade_peso_dict[idade] = []
            idade_peso_dict[idade].append(peso) # Adiciona o peso à lista correspondente à idade

        for idade, pesos in idade_peso_dict.items():
            if 13 <= idade <= 19: 
                media = mean(pesos) # Calcula a média dos pesos para esta idade
                maes_idades.append(idade) # Adiciona a idade da mãe à lista
                bb_pesos.append(media) # Adiciona o peso médio à lista

    corr = correlacao(maes_idades, bb_pesos)
    if arquivo == "Nat2021US.txt":
        print("CORRELAÇÃO:\n")
        print(f"Correlação Total do Dados= {corr:.2f} Media de Peso= {media:.2f}g")

def variancia(Maes_idades: List[float])->float:
    assert len(Maes_idades) >= 2
    n = len(Maes_idades)
    desvios = de_media(Maes_idades)
    return sum_of_squares(desvios) / (n -1)

def desvio_padrao(Maes_idades: List[float])->float:
    return math.sqrt(variancia(Maes_idades))

def covariancia(Maes_idades: List[float], bb_pesos: List[float]) -> float:
    assert len(Maes_idades) == len(bb_pesos)
    return dot(de_media(Maes_idades), de_media(bb_pesos)) / (len(Maes_idades)-1)
Vector = List[float]

def dot(v: Vector, w:Vector)->float:
    assert len(v) == len(w)
    return sum(v_i * w_i for v_i, w_i in zip(v, w))

def sum_of_squares(v: Vector)->float:
    return dot(v, v)

def de_media(Maes_idades: List[float])->List[float]:
    x_bar = mean(Maes_idades)
    return [x - x_bar for x in Maes_idades]

def correlacao(Maes_idades: List[float], bb_pesos: List[float]) -> float:
    stdev_x = desvio_padrao(Maes_idades)
    stdev_y = desvio_padrao(bb_pesos)
    
    if stdev_x > 0 and stdev_y > 0:
        return covariancia(Maes_idades, bb_pesos) / stdev_x / stdev_y
    else:
        return 0

Cria_threads(2, fila_arquivo)  # Cria o numero de threads para processar os arquivos da fila
