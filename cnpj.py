from sys import argv
from urllib.request import urlopen
from os import remove, listdir, rmdir, mkdir
from wget import download
from re import findall
from zipfile import ZipFile
from json import loads, dumps
from sqlite3 import connect, OperationalError
from csv import reader
from tqdm import tqdm
class CNPJ:
    def __init__(self):
        try:
            conf = open("download.json", "r")
            self.files = loads(conf.read())
            conf.close()
        except:
            raise Exception("Erro: arquivo download.json não encontrados.")
        try:
            conf2 = open("data_name.json", "r")
            self.data_name = loads(conf2.read())
            conf2.close()
        except:
            raise Exception("Erro: arquivo data_name.json não encontrados.")
        try:
            conf3 = open("genDB.json", "r")
            self.gendb = loads(conf3.read())
            conf3.close()
        except:
            raise Exception("Erro: arquivo data_name.json não encontrados.")
    def mes(self):
        request_url = urlopen('https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/')
        s = request_url.read().decode("utf-8")
        m = findall(r'<a href="[0-9]+-[0-9]+', s)
        l = []
        for i in m:
            l.append(i[9:len(i)])
        return l
    def download(self, mes):
        if not str(mes) in self.files:
            self.files[str(mes)] = {"Socios5": 0, "Estabelecimentos1": 0, "Socios9": 0, "Municipios": 0, "Empresas3": 0, "Qualificacoes": 0, "Estabelecimentos9": 0, "Motivos": 0, "Empresas2": 0, "Cnaes": 0, "Empresas8": 0, "Estabelecimentos3": 0, "Estabelecimentos6": 0, "Naturezas": 0, "Socios8": 0, "Estabelecimentos2": 0, "Estabelecimentos4": 0, "Estabelecimentos8": 0, "Empresas0": 0, "Socios1": 0, "Empresas4": 0, "Socios3": 0, "Socios4": 0, "Empresas5": 0, "Empresas9": 0, "Socios0": 0, "Estabelecimentos7": 0, "Socios2": 0, "Paises": 0, "Simples": 0, "Socios7": 0, "Empresas6": 0, "Estabelecimentos5": 0, "Socios6": 0, "Empresas1": 0, "Estabelecimentos0": 0, "Empresas7": 0}
        request_url = urlopen('https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/{}/'.format(mes))
        s = request_url.read().decode("utf-8")
        m = findall(r"(\w+).zip", s)
        m = list(set(m))
        verifyPath = False
        for i in listdir():
            if f"download_{mes}" == i:
                verifyPath = True
        if not verifyPath:
            mkdir(f"./download_{mes}")
        for file in m:
            try:
                if self.files[str(mes)][file] == 0:
                    print(f"\nDownloading {file}.zip:")
                    download('https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/{}/{}.zip'.format(mes, file), out=f"./download_{mes}")
                    print(f"\n[+] Unpacking {file}.zip")
                    with ZipFile(f"./download_{mes}/{file}.zip") as zip:
                        zip.extractall(f"./download_{mes}/"+file)
                    remove(f"./download_{mes}/"+file+".zip")
                    self.files[str(mes)][file] = 1
            except KeyboardInterrupt:
                for i in listdir():
                    if ".tmp" in i:
                        remove(i)
                with open("download.json", "w") as conf:
                    conf.write(dumps(self.files))
                exit()
        with open("download.json", "w") as conf:
            conf.write(dumps(self.files))
    def genDB(self, mes):
        def nonum(s):
            if s[-1] in "0123456789":
                return nonum(s[0:len(s)-1])
            else:
                return s
        def lenCsv(c):
            with open(c, "r", encoding="utf-8", errors="replace") as res:
                return sum(1 for _ in res)
        # cria o banco de dados (data.db)
        verifyPath = False
        for i in listdir():
            if "db" == i:
                verifyPath = True
        if not verifyPath:
            mkdir("./db")
        con = connect(f"./db/data_{mes}.db")
        # cria as tabela
        cur = con.cursor()
        for type in self.data_name:
            sql = f"CREATE TABLE IF NOT EXISTS {type} ("
            for i in range(0, len(self.data_name[type])):
                sql += f"{self.data_name[type][i]}," if i < len(self.data_name[type])-1 else f"{self.data_name[type][i]});"
            cur.execute(sql)
        con.commit()
        # insere dados nas tabelas
        for fileName in listdir(f"./download_{mes}"):
            if self.gendb[fileName][0] == 0:
                print("\n[+] filling data base whith "+fileName+":")
                for f in listdir(f"./download_{mes}/"+fileName):
                    with open(f"./download_{mes}/"+fileName+"/"+f, "r", encoding='utf-8', errors='replace') as res:
                        leitor = reader(res, delimiter=';')
                        l = lenCsv(f"./download_{mes}/"+fileName+"/"+f)
                        if self.gendb[fileName][1] < l:
                            bar = tqdm(total=l-self.gendb[fileName][1])
                            for linha in leitor:
                                try:
                                    sql = f"INSERT OR IGNORE INTO {nonum(fileName)} VALUES ("
                                    for i in range(0, len(linha)):
                                        sql += "\""+linha[i]+"\"," if i < len(linha)-1 else "\""+linha[i]+"\");"
                                    cur.execute(sql)
                                except OperationalError:
                                    try:
                                        sql = f"INSERT OR IGNORE INTO {nonum(fileName)} VALUES ("
                                        for i in range(0, len(linha)):
                                            s = ""
                                            for l in linha[i]:
                                                s += l if l != "\"" else "'"
                                            sql += "\""+s+"\"," if i < len(linha)-1 else "\""+s+"\");"
                                        cur.execute(sql)
                                    except KeyboardInterrupt:
                                        con.commit()
                                        con.close()
                                        with open("genDB.json", "w") as r:
                                            r.write(dumps(self.gendb))
                                        exit()
                                except KeyboardInterrupt:
                                    con.commit()
                                    con.close()
                                    with open("genDB.json", "w") as r:
                                        r.write(dumps(self.gendb))
                                    exit()  
                                bar.update(1)
                                self.gendb[fileName][1] += 1
                            bar.close()
                self.gendb[fileName][0] = 1
        con.commit()
        con.close()
        with open("genDB.json", "w") as r:
            r.write(dumps(self.gendb))
    def nav(self, n):
        mes = n[n.find(":")+1:len(n)] if ":" in n else None
        match n:
            case "mes":
                for i in self.mes():
                    print(i)
            case n if "download" in n:
                if mes == None:
                    print("Insira o mes!")
                    exit()
                elif "all" == mes:
                    for m in self.mes():
                        self.download(m)
                elif ("del" in mes) and ("[" in mes) and ("]" in mes):
                    m = mes[mes.find("[")+1:mes.find("]")]
                    if m == "all":
                        for file in listdir():
                            if "download_" in file:
                                for i in listdir(f"./{file}"):
                                    for j in listdir(f"./{file}/{i}"):
                                        remove(f"./{file}/{i}/{j}")
                                    rmdir(f"./{file}/{i}")
                                rmdir(f"./{file}")
                                del self.files[str(file[file.find("_")+1:len(file)])]
                        for i in self.gendb:
                            self.gendb[i] = [0, 0]
                        with open("genDB.json", "w") as r:
                            r.write(dumps(self.gendb))
                        with open("download.json", "w") as r:
                            r.write(dumps(self.files))
                    elif "-" in m:
                        try:
                            for p in listdir(f"./download_{m}"):
                                for i in listdir(f"./download_{m}/{p}"):
                                    remove(f"./download_{m}/{p}/{i}")
                                rmdir(f"./download_{m}/{p}")
                            rmdir(f"./download_{m}")
                        except FileNotFoundError:
                            print("Erro: digite o mes que voce baixou entre cochetes")
                            exit()
                        for i in self.gendb:
                            self.gendb[i] = [0, 0]
                        with open("genDB.json", "w") as r:
                            r.write(dumps(self.gendb))
                        del self.files[str(m)]
                        with open("download.json", "w") as r:
                            r.write(dumps(self.files))
                elif ("-" in mes) and (not "[" in mes) and (not "]" in mes):
                    try:
                        self.download(mes)
                    except Exception as e:
                        print(f"Erro: {e}")
            case n if "db" in n:
                if (mes == "all") and (not "[" in mes) and (not "]" in mes):
                    for i in listdir():
                        if "download_" in i:
                            m = i[i.find("_")+1:len(i)]
                            cont = 0
                            cont2 = 0
                            for i1 in self.gendb:
                                cont += self.gendb[i1][0]
                            for i2 in self.files[m]:
                                cont2 += self.files[m][i2]
                            if cont != len(self.gendb):
                                if cont2 == len(self.files[m]):
                                    try:
                                        self.genDB(m)
                                    except Exception as e:
                                        print(f"Erro: {e}")
                                else:
                                    print(f"Download de (./download_{m}) INCOMPLETO!")
                            else:
                                print(f"Data base (mes = {mes}) COMPLETA!")
                elif ("-" in mes) and (not "[" in mes) and (not "]" in mes):
                    cont = 0
                    for i in self.gendb:
                        cont += self.gendb[i][0]
                    if cont != len(self.gendb):
                        try:
                            self.genDB(mes)
                        except Exception as e:
                            print(f"Erro: {e}")
                    else:
                        print(f"Data base (mes = {mes}) COMPLETA!")
            case _:
                print(f"Comando: {n}, não existe!")
        del mes
cnpj = CNPJ()
cnpj.nav(argv[1])