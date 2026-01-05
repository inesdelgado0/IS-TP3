import os
import csv
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
NOME_BUCKET = "carros"

def crawler_carros_eletricos():
    base_url = "https://auto.sapo.pt/carros-usados?combustivel=electrico"
    lista_total_carros = []
    
    p = 1 # Começamos na página 1
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    print(f"\nA iniciar extração...")

    while True:
        url_pagina = f"{base_url}&p={p}"
        print(f"-> A fazer scraping da página {p}...")
        
        try:
            response = requests.get(url_pagina, headers=headers)
            if response.status_code != 200:
                print(f"Falha na página {p}. A terminar...")
                break
                
            soup = BeautifulSoup(response.text, 'html.parser')
            anuncios = soup.find_all('div', class_='description')

            if not anuncios:
                print(f"Não há mais anúncios. Fim da recolha na página {p-1}.")
                break

            for item in anuncios:
                link_tag = item.find('a', itemprop='url')
                titulo = link_tag.get_text(strip=True) if link_tag else "N/A"
                
                features = item.find('ul', class_='push-bottom')
                if features:
                    li_items = features.find_all('li')
                    ano = li_items[0].get_text(strip=True) if len(li_items) > 0 else "N/A"
                    km = li_items[1].get_text(strip=True).replace('km', '').replace('.', '').strip() if len(li_items) > 1 else "0"
                else:
                    ano, km = "N/A", "0"

                preco_div = item.find('div', class_='price')
                preco = preco_div.find('span').get_text(strip=True).replace('€', '').replace('.', '').strip() if preco_div else "0"

                lista_total_carros.append({
                    "marca_modelo": titulo,
                    "preco_eur": preco,
                    "ano": ano,
                    "quilometros": km,
                    "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
            
            # 3. INCREMENTAR a página para a próxima volta
            p += 1
            time.sleep(1)

        except Exception as e:
            print(f"Erro na página {p}: {e}")
            break

    if not lista_total_carros:
        print("Nenhum dado extraído.")
        return

    filename = "carros_eletricos.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=lista_total_carros[0].keys())
        writer.writeheader()
        writer.writerows(lista_total_carros)
    
    print(f"\nCSV gerado com {len(lista_total_carros)} carros extraídos de {p-1} páginas.")

    with open(filename, "rb") as f:
        nome_remoto = f"carros_eletricos{int(datetime.now().timestamp())}.csv"
        supabase.storage.from_(NOME_BUCKET).upload(
            path=nome_remoto,
            file=f,
            file_options={"content-type": "text/csv"}
        )
    print(f"Ficheiro enviado para o Supabase: {nome_remoto}")

if __name__ == "__main__":
    crawler_carros_eletricos()