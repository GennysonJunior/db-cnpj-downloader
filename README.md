# db-cnpj-downloader
- Programa em python que baixa uma base de dados publicos de CNPJ's do governo e transforma em arquivo SQLITE (`data_[ano-mes].db`) com a mesma configuração de tabelas do governo (você pode ver essa configuração em `layout data_raw.pdf`).
- Este programa guarda os status de download e criação do banco de dados nestes arqivos: `download.json` e `genDB.jsom`, para que você possa parar o processo à hora que quiser e continuar em uma outro momento.
- Ao terminar, o programa cria dois tipos de pastas, uma `/db` onde fica os arquivos SQLITE e outras `/download_[ano-mes]` onde é baixado os dados (csv) publicos do governo.
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
> Rode o seguinte comando para listar os meses existemtes para download.
```bash
py cnpj.py mes
```
-- DOWNLOAD
> Para baixar todos os meses, digite o seguinte comando.
```bash
py cnpj.py download:all
```
> Para baixar um mes específico, digite o seguinte comando `py cnpj.py download:ano-mes`.
```bash
py cnpj.py download:2024-05
```
> Para excluir uma pasta de download, digite o segunte comando `py cnpj.py download:del[ano-mes]`.
```bash
py cnpj.py download:del[2024-05]
```
> Para excluir todas as pastas de download, digite o seguinte comando.
```bash
py cnpj.py download:del[all]
```

-- DATA BESE (SQLITE)
> Para gerar o banco de dados de um mes somente, digite o seguinte comando `py cnpj.py db:[ano-mes]`.
```bash
py cnpj.py db:[2024-05]
```
> Para gerar os bancos de dados de todos os meses que você baixou, digite o seguinte comando.
```bash
py cnpj.py db:all
```