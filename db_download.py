from sqlite3 import connect, OperationalError, ProgrammingError
from zipfile import ZipFile
from csv import reader
from json import loads, dumps
from wget import download
from sys import argv
from os import remove, listdir
from urllib.request import urlopen
from re import findall
from tqdm import tqdm
#██████████████████████████████████████████████████████████████████████████████████████████████████████████████
class CNPJ:
    def __init__(self):
        try:
            conf = open("confg_download.json", "r")
            self.files = loads(conf.read())
            conf.close()
        except:
            raise Exception("Erro: arquivo confg_download.json não encontrados.")

        try:
            conf2 = open("confg_data.json", "r")
            self.dataType = loads(conf2.read())
            conf2.close()
        except:
            raise Exception("Erro: arquivo confg_data.json não encontrado.")
        
        try:
            conf3 = open("confg_newData.json", "r")
            self.newDataType = loads(conf3.read())
            conf3.close()
        except:
            raise Exception("Erro: arquivo confg_data.json não encontrado.")

    def updateConfg(self):
        request_url = urlopen('https://dadosabertos.rfb.gov.br/CNPJ/') 
        s = request_url.read().decode("utf-8")
        m = findall(r"(\w+).zip", s)
        # resetar "download" e "dataPush"
        for i in m:
            self.files = {i: {"download": 0, "dataPush": 0}}
        # escrever mudança confg_download.json
        with open("confg_data.json", "w") as res:
            print("[*] reset confg_download.json")
            res.write(dumps(self.files))
        # resetar "it" e "state"
        for nome in self.newDataType:
            self.newDataType[nome]["it"] = 1
            self.newDataType[nome]["state"] = 0
        # escrever mudança confg_newData.json
        with open("confg_newData.json", "w") as res:
            print("[*] reset confg_newData.json")
            res.write(dumps(self.newDataType))

    def remove(self):
        num_data_downloaded = 0
        for file in self.files:
            if self.files[file]["download"] == 1:
                num_data_downloaded += 1
        if num_data_downloaded == len(self.files):
            # apaga os arquivos csv antigos
            for dir in listdir("./download"):
                for d in listdir("./download/"+dir):
                    remove("./download/"+dir+"/"+d)
                print("[-] deleting "+dir)
                remove("./download/"+dir)

    # baixa os arquivos csv do site governo, extrai, organiza em pastas e exclui o arquivos zip
    def downloader(self):
        kbi = False
        for file in self.files:
            try:
                if self.files[file]["download"] == 0:
                    print(f"\nDownloading {file}:")
                    download("https://dadosabertos.rfb.gov.br/CNPJ/"+file, out="./download")
                    print(f"\n[+] Unpacking {file}")
                    with ZipFile("./download/"+file) as zip:
                        zip.extractall("./download/"+file[0:-4])
                    remove("./download/"+file)
                    self.files[file]["download"] = 1
            except KeyboardInterrupt:
                kbi = True
                for f in listdir():
                    if file in f:
                        remove(f)
                break
            except Exception as e:
                print(f"Erro no download do arquivo {file}\nMotivo: {e}")
        if kbi:
            with open("confg_download.json", "w") as conf:
                conf.write(dumps(self.files))
            exit()
        else:
            with open("confg_download.json", "w") as conf:
                conf.write(dumps(self.files))
        
        sumFD = 0
        for file in self.files:
            if self.files[file]["download"] == 1:
                sumFD += 1
        if sumFD == len(self.files):
            print("[*] Download finished.")

    def genDbRaw(self):
        def nonum(s):
            if s[-1] in "0123456789":
                return nonum(s[0:len(s)-1])
            else:
                return s
        # ver se data_raw.db foi criado
        dr = False
        for i in listdir():
            if "db" == i:
                for i1 in listdir("./db"):
                    if "data_raw.db" == i1:
                        dr = True
        if not dr:
            print("\n\n[*] Creating raw data base...")
        # cria o banco de dados (data.db)
        con = connect("./db/data_raw.db")
        # cria as tabela
        cur = con.cursor()
        for type in self.dataType:
            sql = f"CREATE TABLE IF NOT EXISTS {type} ("
            for i in range(0, len(self.dataType[type])):
                sql += f"{self.dataType[type][i]}," if i < len(self.dataType[type])-1 else f"{self.dataType[type][i]});"
            cur.execute(sql)
        con.commit()
        # insere dados nas tabelas
        for fileName in listdir("./download"):
            if self.files[fileName]["dataPush"] == 0:
                print("[+] filling data base whith "+fileName+":")
                for f in listdir("./download/"+fileName):
                    with open("./download/"+fileName+"/"+f, "r", encoding='utf-8', errors='replace') as res:
                        leitor = reader(res, delimiter=';')
                        for linha in tqdm(leitor):
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
                                except ProgrammingError:
                                    pass
                                except KeyboardInterrupt:
                                    con.close()
                                    with open("confg_download.json", "w") as res:
                                        res.write(dumps(self.files))
                                    exit()
                            except ProgrammingError:
                                pass
                            except KeyboardInterrupt:
                                con.close()
                                with open("confg_download.json", "w") as res:
                                    res.write(dumps(self.files))
                                exit()
                    con.commit()
                self.files[fileName]["dataPush"] = 1
        con.close()
        with open("confg_download.json", "w") as res:
            res.write(dumps(self.files))
        print("\n\n[*] Raw data base created.\n")

    def genNewDb(self):
        # verifica se data.db foi criado 
        dr = False
        for i in listdir():
            if "db" == i:
                for i1 in listdir("./db"):
                    if "data.db" == i1:
                        dr = True
        if not dr:
            print("\n\n[*] Creating new data base...")
        # cria o banco de dados (data.db)
        newcon = connect("./db/data.db")
        con = connect("./db/data_raw.db")
        cur = con.cursor()
        # cria as tabela
        newcur = newcon.cursor()
        for type in self.newDataType:
            sql = f"CREATE TABLE IF NOT EXISTS {type} ("
            for i in range(0, len(self.newDataType[type]["data"])):
                sql += f"{self.newDataType[type]["data"][i]}," if i < len(self.newDataType[type]["data"])-1 else f"{self.newDataType[type]["data"][i]});"
            newcur.execute(sql)
        newcon.commit()
        try:
            # inserir dados na tabela Simples
            if self.newDataType["Simples"]["state"] == 0:
                print("[*] chenge data Simples...")
                cur.execute("SELECT COUNT(*) FROM Simples;")
                t = cur.fetchall()[0][0]
                print("[+] filling new data base whith Simples:")
                cont = self.newDataType["Simples"]["it"]
                pbar = tqdm(total=t-cont+1)
                while cont <= t:
                    cur.execute(f"SELECT * FROM Simples WHERE ROWID == {cont};")
                    ress = cur.fetchall()

                    newcon.execute(f"INSERT OR IGNORE INTO Simples VALUES (?,?,?,?,?,?,?);", ress[0])
                    self.newDataType["Simples"]["it"] = cont
                    pbar.update(1)
                    cont += 1
                newcon.commit()
                self.newDataType["Simples"]["state"] = 1
            # inserir dados na tabela Socios
            if self.newDataType["Socios"]["state"] == 0:
                print("\n[*] chenge data Socios...")
                cur.execute("SELECT COUNT(*) FROM Socios;")
                t = cur.fetchall()[0][0]
                print("[+] filling new data base whith Socios:")
                cont = self.newDataType["Socios"]["it"]
                pbar = tqdm(total=t-cont+1)
                while cont <= t:
                    cur.execute(f"SELECT * FROM Socios WHERE ROWID == {cont};")
                    ress = cur.fetchall()

                    cur.execute(f"SELECT descricao FROM Qualificacoes WHERE id == '{ress[0][4]}';")
                    rq = cur.fetchall()
                    q = rq[0][0] if len(rq) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Qualificacoes WHERE id == '{ress[0][9]}';")
                    rqr = cur.fetchall()
                    qr = rqr[0][0] if len(rqr) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Paises WHERE id == '{ress[0][6]}';")
                    rp = cur.fetchall()
                    p = rp[0][0] if len(rp) > 0 else ""

                    ids = {
                        "1": "PESSOA JURÍDICA",
                        "2": "PESSOA FÍSICA",
                        "3": "ESTRANGEIRO",
                        "": ""
                    }
                    fe = {
                        "1": "0 a 12 anos",
                        "2": "13 a 20 anos",
                        "3": "21 a 30 anos",
                        "4": "31 a 40 anos",
                        "5": "41 a 50 anos",
                        "6": "51 a 60 anos",
                        "7": "61 a 70 anos",
                        "8": "71 a 80 anos",
                        "9": "maiores de 80 anos",
                        "0": "não se aplica",
                        "": ""
                    }

                    l = []
                    num = 0
                    for i in ress[0]:
                        if num == 1:
                            l.append(ids[i])
                        elif num == 4:
                            l.append(q)
                        elif num == 6:
                            l.append(p)
                        elif num == 9:
                            l.append(qr)
                        elif num == 10:
                            l.append(fe[i])
                        else:
                            l.append(i)
                        num += 1
                    newcon.execute(f"INSERT OR IGNORE INTO Socios VALUES (?,?,?,?,?,?,?,?,?,?,?);", l)
                    pbar.update(1)
                    cont += 1
                    self.newDataType["Socios"]["it"] = cont
                newcon.commit()
                self.newDataType["Socios"]["state"] = 1
            # inserir dados na tabela Empresas
            if self.newDataType["Empresas"]["state"] == 0:
                print("\n[*] chenge data Empresas...")
                cur.execute("SELECT COUNT(*) FROM Empresas;")
                t = cur.fetchall()[0][0]
                print("[+] filling new data base whith Empresas:")
                cont = self.newDataType["Empresas"]["it"]
                pbar = tqdm(total=t-cont+1)
                while cont <= t:
                    cur.execute(f"SELECT * FROM Empresas WHERE ROWID == {cont};")
                    ress = cur.fetchall()

                    cur.execute(f"SELECT descricao FROM Naturezas WHERE id == '{ress[0][2]}';")
                    rn = cur.fetchall()
                    n = rn[0][0] if len(rn) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Qualificacoes WHERE id == '{ress[0][3]}';")
                    rq = cur.fetchall()
                    q = rq[0][0] if len(rq) > 0 else ""

                    pe = {
                        "01": "NÃO INFORMADO",
                        "02": "MICRO EMPRESA",
                        "03": "EMPRESA DE PEQUENO PORTE",
                        "05": "DEMAIS",
                        "": ""
                    }

                    l = []
                    num = 0
                    for i in ress[0]:
                        if num == 2:
                            l.append(n)
                        elif num == 3:
                            l.append(q)
                        elif num == 5:
                            l.append(pe[i])
                        else:
                            l.append(i)
                        num += 1
                    newcon.execute(f"INSERT OR IGNORE INTO Empresas VALUES (?,?,?,?,?,?,?);", l)
                    pbar.update(1)
                    cont += 1
                    self.newDataType["Empresas"]["it"] = cont
                newcon.commit()
                self.newDataType["Empresas"]["state"] = 1
            # inserir dados na tabela Estabelecimentos
            if self.newDataType["Estabelecimentos"]["state"] == 0:
                print("\n[*] chenge data Estabelecimentos...")
                cur.execute("SELECT COUNT(*) FROM Estabelecimentos;")
                t = cur.fetchall()[0][0]
                print("[+] filling new data base whith Estabelecimentos:")
                cont = self.newDataType["Estabelecimentos"]["it"]
                pbar = tqdm(total=t-cont+1)
                while cont <= t:
                    cur.execute(f"SELECT * FROM Estabelecimentos WHERE ROWID == {cont};")
                    ress = cur.fetchall()

                    cur.execute(f"SELECT descricao FROM Motivos WHERE id == '{ress[0][7]}';")
                    rm = cur.fetchall()
                    m = rm[0][0] if len(rm) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Paises WHERE id == '{ress[0][9]}';")
                    rp = cur.fetchall()
                    p = rp[0][0] if len(rp) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Cnaes WHERE id == '{ress[0][11]}';")
                    rcf = cur.fetchall()
                    cf = rcf[0][0] if len(rcf) > 0 else ""
                    cur.execute(f"SELECT descricao FROM Municipios WHERE id == '{ress[0][20]}';")
                    rmu = cur.fetchall()
                    mu = rmu[0][0] if len(rmu) > 0 else ""

                    imf = {
                        "1": "MATRIZ",
                        "2": "FILIAL",
                        "": ""
                    }
                    sc = {
                        "01": "NULA",
                        "02": "ATIVA",
                        "03": "SUSPENSA",
                        "04": "INAPTA",
                        "08": "BAIXADA",
                        "": ""
                    }

                    l = []
                    num = 0
                    for i in ress[0]:
                        if num == 3:
                            l.append(imf[i])
                        elif num == 5:
                            l.append(sc[i])
                        elif num == 7:
                            l.append(m)
                        elif num == 9:
                            l.append(p)
                        elif num == 11:
                            l.append(i+": "+cf)
                        elif num == 12:
                            cs = ""
                            for j in i.split(","):
                                if len(j) > 0:
                                    cur.execute(f"SELECT descricao FROM Cnaes WHERE id == '{j}';")
                                    cs += j+": "+cur.fetchall()[0][0]+"; "
                            l.append(cs)
                        elif num == 20:
                            l.append(mu)
                        else:
                            l.append(i)
                        num += 1
                    newcon.execute(f"INSERT OR IGNORE INTO Estabelecimentos VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);", l)
                    pbar.update(1)
                    cont += 1
                    self.newDataType["Estabelecimentos"]["it"] = cont
                newcon.commit()
                self.newDataType["Estabelecimentos"]["state"] = 1
        except KeyboardInterrupt:
            newcon.commit()
            newcon.close()
            con.close()
            with open("confg_newData.json", "w") as r:
                r.write(dumps(self.newDataType))
            exit()
        except Exception as e:
            newcon.commit()
            newcon.close()
            con.close()
            with open("confg_newData.json", "w") as r:
                r.write(dumps(self.newDataType))
            print(e)
            exit()
        newcon.close()
        con.close()
        with open("confg_newData.json", "w") as r:
            r.write(dumps(self.newDataType))
        print("\n[*] New data base created!")

if __name__ == "__main__":
    cnpj = CNPJ()
    for arg in argv[1:len(argv)]:
        if arg[0] == "-":
            match arg:
                case "-b" | "--raw-data-base":
                    cnpj.genDbRaw()
                case "-d" | "--download":
                    cnpj.downloader()
                case "-u" | "--update":
                    cnpj.updateConfg()
                case "-r" | "--remove":
                    cnpj.remove()
                case "-n" | "--new-data-base":
                    cnpj.genNewDb()
                case "-?" | "--help":
                    print("""\n
USE:
    py db_download.py <COMMANDS>

COMMAND:
    start...................Comando para fazer o download dos arquivos e a criação do banco sqlite.
    remove..................Comando para apagar todos os arquivos da pasta \"download\" e atualizar o arquivo confg_download.json.
    
    -d    --download........Comando para fazer o download dos arquivos do site do governo.
    -b    --raw-data-base...Comando para fazer o banco sqlite caso os arquivos já estejam todos baixados.
    -u    --update..........Comando para atualizar o arquivo confg_download.json, caso tenha mais arquivos no site do governo (Atenção: este comando reseta todo o arquivo confg_download.json, perdendo todo o estado de download anterior; Recomendação: executar este comando sempre que possivel).
    -r    --remove..........Comando para apagar todos os arquivos da pasta \"download\".
    -n    --new-data-base...Comando para gerar um novo banco de dados sqlite com tabelas mais simples.
    -?    --help............Comando de ajuda.
                        \n""")
                case _:
                    raise Exception(f"Erro: comando ({arg}) não existe, deigite: \"py db_download.py -?\"")
        elif arg == "start":
            cnpj.downloader()
            cnpj.genDbRaw()
            cnpj.genNewDb()
        elif arg == "remove":
            cnpj.remove()
            cnpj.updateConfg()
        else:
            raise Exception(f"Erro: comando ({arg}) não existe, deigite: \"py db_download.py -?\"")
