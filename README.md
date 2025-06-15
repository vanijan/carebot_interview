# Carebot coding assignment - Jan Vanik

## Teoretická část
Odpovědi na teoretickou část jsou v souboru Teorie.pdf

## SQL

Queries jsou ve složce sql

## Python
### Před spuštěním
- (virtuální prostředí)
```bash
pip install -r requirements.txt
```
vytvořit .env file s proměnnými
PG_HOST,
PG_USER,
PG_PORT,
PG_DATABASE,
PG_PASSWORD,
AZURE_CONNECTION_STRING,
PDF_TARGET_DIR,
JOINED_PDF_TARGET_DIR


### Pro spuštění 
```bash
python src/aggregate_pdf_reports.py [--date YYYY-MM-DD] [--delta DAYS]
```
- --date defaultuje na dnešek
- --delta defaultuje na 14

### Testy
Generovány celé pomocí LLM, občas potřebovaly trochu pomoct z mé strany :)
Základní
```bash
pytest
```
Pro získání coverage reportu
```bash
pytest --cov=your_package_or_module tests/
```
Aktuální coverage je 90 %, není totiž pokrytý __name__ == __main__:

### Poznámky k implementaci/potenciální nedostatky
- soubory jsem filtroval časově jak v databázi tak poté podle data pořízení snímků, aby to bylo rychlejší
- mnoho souborů (jak pdf tak dcm) často v Azure storage nebyla, nevím jestli jsem pdf string skládal špatně, nebo skutečně chybí
