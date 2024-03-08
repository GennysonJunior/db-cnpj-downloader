# db-cnpj-downloader
- Programa em python que baixa uma base de dados publicos de CNPJ's do governo e transforma em 2 arquivo SQLITE, um arquivo intermediário `data_raw.db` com a mesma configuração de tabelas do governo (você pode ver essa configuração em `layout data_raw.pdf`) e outro arquivo `data.db` com 4 tabelas (Empresas, Simples, Socios e Estabelecimentos).
- Este programa guarda os status de download e criação do banco de dados nestes arqivos: `confg_download.json` e `confg_newData.jsom`, para que você possa parar o processo à hora que quiser e continuar em uma outro momento.
- Ao terminar, o programa cria duas pasta, uma `/db` onde fica os arquivos SQLITE e outra `/download` onde é baixado os dados (csv) publicos do governo.
---

# Como instalar
> Instalar o [Python3](https://www.python.org/).

> Clone o repositório.
```bash
git clone https://github.com/GennysonJunior/db-cnpj-downloader.git
```
> Isntale os requerimentos com o comando a baixo.
```bash
cd /db-cnpj-downloader
pip install -r requirements.txt
```
---
# Como usar
> Rode o seguinte comando para iniciar.
```bash
py db_download.py start
```
> Se você quiser resetar os status de uso e apagar os arquivos baixados do site do governo, use o comando abaixo.

> (este comando **não** remove os arquivos da pasta `/db`)
```bash
py db_download.py remove
```
> Você pode usar o seguinte comando para listar as opções de comandos, e usar as opções que desejar.
```bash
py db_download.py -?
```
