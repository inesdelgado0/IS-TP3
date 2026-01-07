import os
import csv
import requests
import time
import re
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

# Configurações Iniciais
load_dotenv()
url = os.getenv("SUPABASE_URL")
if url and not url.endswith('/'): url += '/'
supabase = create_client(url, os.getenv("SUPABASE_KEY"))
NOME_BUCKET = "carros"

def limpar_numero(texto):
    if not texto or texto == "N/A": return "0"
    return re.sub(r'\D', '', texto)

def crawler_carros():
    base_url = "https://auto.sapo.pt/carros-usados?combustivel=diesel%7Cgasolina&orderby=1"
    lista_total_carros = []
    
    p = 1 
    max_registos = 100
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

    print(f"\nScraping de {max_registos} carros do site autoSapo...")

    while len(lista_total_carros) < max_registos:
        url_pagina = f"{base_url}&p={p}"
        try:
            response = requests.get(url_pagina, headers=headers, timeout=10)
            if response.status_code != 200: break
            
            soup = BeautifulSoup(response.text, 'html.parser')
            anuncios = soup.find_all('div', class_='description')
            if not anuncios: break

            for item in anuncios:
                if len(lista_total_carros) >= max_registos: break

                try: 
                    h3_tag = item.find('h3', itemprop='name')
                    a_tag = h3_tag.find('a') if h3_tag else None
                    if not a_tag: continue

                    url_anuncio = "https://auto.sapo.pt" + a_tag['href']
                    marca_modelo = a_tag.find(string=True, recursive=False).strip()
                    
                    # Dados básicos
                    features = item.find('ul', class_='push-bottom')
                    li_list = features.find_all('li') if features else []
                    ano = li_list[0].get_text(strip=True) if len(li_list) > 0 else "0"
                    
                    km_raw = li_list[1].get_text(strip=True) if len(li_list) > 1 else "0"
                    km = limpar_numero(km_raw)
                    
                    comb = li_list[2].get_text(strip=True) if len(li_list) > 2 else "N/A"
                    
                    preco_span = item.select_one('.price span')
                    preco = limpar_numero(preco_span.get_text(strip=True)) if preco_span else "0"

                    # Scraping dentro do anuncio
                    localidade, cilindrada, caixa, segmento, potencia = "N/A", "0", "N/A", "N/A", "0"
                    
                    res_detalhe = requests.get(url_anuncio, headers=headers, timeout=10)
                    s_detalhe = BeautifulSoup(res_detalhe.text, 'html.parser')

                    # Localidade (Region + Country)
                    trader = s_detalhe.find('div', id='trader')
                    if trader:
                        region = trader.find('span', itemprop='addressRegion')
                        country = trader.find('span', itemprop='addressCountry')
                        reg_text = region.get_text(strip=True) if region else ""
                        cou_text = country.get_text(strip=True) if country else ""
                        localidade = f"{cou_text}, {reg_text}" if reg_text and cou_text else (cou_text or reg_text or "N/A")
                    
                    # Características Técnicas
                    caract_ul = s_detalhe.find('ul', class_='column-group half-gutters')
                    if caract_ul:
                        li_cil = caract_ul.find('li', class_='cilindrada')
                        if li_cil: cilindrada = limpar_numero(li_cil.find('strong').get_text(strip=True))
                        
                        li_caixa = caract_ul.find('li', class_='caixa')
                        if li_caixa: caixa = li_caixa.find('strong').get_text(strip=True)
                        
                        li_seg = caract_ul.find('li', class_='body-type')
                        if li_seg: segmento = li_seg.find('strong').get_text(strip=True)

                    
                    resume = s_detalhe.find('div', id='vehicle-resume')
                    if resume:
                        for li in resume.find_all('li'):
                            if 'cv' in li.get_text().lower():
                                potencia = limpar_numero(li.get_text(strip=True))

                    lista_total_carros.append({
                        "marca_modelo": marca_modelo,
                        "preco_eur": preco,
                        "ano": ano,
                        "quilometros": km,
                        "combustivel": comb,
                        "localidade": localidade,
                        "cilindrada": cilindrada,
                        "potencia": potencia,
                        "caixa": caixa,
                        "segmento": segmento
                    })
                except Exception:
                    continue

            print(f"   > Página {p} concluída. Total: {len(lista_total_carros)}/{max_registos}")
            p += 1
            time.sleep(0.4) 

        except Exception as e:
            print(f"Erro na página {p}: {e}")
            break

    # Gravar CSV e enviar para a supabase
    if lista_total_carros:
        filename = "carros.csv"
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=lista_total_carros[0].keys())
            writer.writeheader()
            writer.writerows(lista_total_carros)

        try:
            with open(filename, "rb") as f:
                nome_remoto = f"carros_{int(datetime.now().timestamp())}.csv"
                supabase.storage.from_(NOME_BUCKET).upload(nome_remoto, f)
            print(f"\nFicheiro {nome_remoto} enviado para a Supabase com sucesso.")
            
            os.remove(filename)
        except Exception as e:
            print(f"Erro ao enviar para o Supabase: {e}")

if __name__ == "__main__":
    # Simula a periodicidade (ex: corre a cada 30 minutos)
    MINUTOS_ESPERA = 60 
    while True:
        crawler_carros()
        print(f"\nCiclo terminado. Próxima recolha de dados em {MINUTOS_ESPERA} minutos...")
        time.sleep(MINUTOS_ESPERA * 60)