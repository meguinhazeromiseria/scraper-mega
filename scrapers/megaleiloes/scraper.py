#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRAPER SIMPLIFICADO
Mapeamento direto: URL do site → Tabela do banco
Sem IA, sem keywords, apenas categorias oficiais do MegaLeilões
"""

import sys
import json
import time
import random
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Optional, Tuple

# Adiciona pasta scrapers/ ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from normalizer import normalize_items

# Imports do scraper
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


class MegaLeiloesScraper:
    """Scraper com mapeamento direto de categorias do site"""
    
    def __init__(self):
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        # MAPEAMENTO DIRETO: (url_path, tabela_destino, nome_exibicao, campos_extras)
        self.sections = [
            # ========== VEÍCULOS (6 tipos) ==========
            ('veiculos/aeronaves', 'veiculos', 'Aeronaves', {'vehicle_type': 'aeronave'}),
            ('veiculos/barcos', 'veiculos', 'Barcos', {'vehicle_type': 'barco'}),
            ('veiculos/caminhoes', 'veiculos', 'Caminhões', {'vehicle_type': 'caminhao'}),
            ('veiculos/carros', 'veiculos', 'Carros', {'vehicle_type': 'carro'}),
            ('veiculos/motos', 'veiculos', 'Motos', {'vehicle_type': 'moto'}),
            ('veiculos/onibus', 'veiculos', 'Ônibus', {'vehicle_type': 'onibus'}),
            
            # ========== IMÓVEIS (14 tipos) ==========
            ('imoveis/apartamentos', 'imoveis', 'Apartamentos', {'property_type': 'apartamento'}),
            ('imoveis/casas', 'imoveis', 'Casas', {'property_type': 'casa'}),
            ('imoveis/galpoes--industriais', 'imoveis', 'Galpões Industriais', {'property_type': 'galpao_industrial'}),
            ('imoveis/glebas', 'imoveis', 'Glebas', {'property_type': 'gleba'}),
            ('imoveis/deposito-de-garagem', 'imoveis', 'Depósito de Garagem', {'property_type': 'deposito_garagem'}),
            ('imoveis/hospitais', 'imoveis', 'Hospitais', {'property_type': 'hospital'}),
            ('imoveis/hoteis', 'imoveis', 'Hotéis', {'property_type': 'hotel'}),
            ('imoveis/imoveis-comerciais', 'imoveis', 'Imóveis Comerciais', {'property_type': 'comercial'}),
            ('imoveis/imoveis-rurais', 'imoveis', 'Imóveis Rurais', {'property_type': 'rural'}),
            ('imoveis/outros', 'imoveis', 'Outros Imóveis', {'property_type': 'outro'}),
            ('imoveis/resorts', 'imoveis', 'Resorts', {'property_type': 'resort'}),
            ('imoveis/terrenos-e-lotes', 'imoveis', 'Terrenos e Lotes', {'property_type': 'terreno_lote'}),
            ('imoveis/terrenos-para-incorporacao', 'imoveis', 'Terrenos p/ Incorporação', {'property_type': 'terreno_incorporacao'}),
            ('imoveis/vagas-de-garagem', 'imoveis', 'Vagas de Garagem', {'property_type': 'vaga_garagem'}),
            
            # ========== ELETRODOMÉSTICOS ==========
            ('bens-de-consumo/eletrodomesticos', 'eletrodomesticos', 'Eletrodomésticos', {}),
            
            # ========== TECNOLOGIA ==========
            ('bens-de-consumo/eletronicos', 'tecnologia', 'Eletrônicos', {}),
            
            # ========== MÓVEIS ==========
            ('bens-de-consumo/moveis', 'moveis_decoracao', 'Móveis', {}),
            
            # ========== INDUSTRIAL ==========
            ('industrial/maquinas', 'industrial_equipamentos', 'Máquinas Industriais', {}),
            
            # ========== ANIMAIS (2 tipos) ==========
            ('animais/cavalos', 'animais', 'Cavalos', {'animal_type': 'cavalo'}),
            ('animais/gado-bovino', 'animais', 'Gado Bovino', {'animal_type': 'gado_bovino'}),
            
            # ========== DIVERSOS ==========
            ('outros/diversos', 'diversos', 'Diversos', {}),
            
            # ========== ARTES ==========
            ('outros/obras-de-arte', 'artes_colecionismo', 'Obras de Arte', {}),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_table': defaultdict(int),
            'by_section': {},
            'duplicates': 0,
        }
    
    def scrape(self) -> dict:
        """
        Scrape completo do MegaLeilões
        Returns: dict com items agrupados por tabela
        """
        print("\n" + "="*60)
        print("🟢 MEGALEILÕES - SCRAPER SIMPLIFICADO")
        print("="*60)
        
        items_by_table = defaultdict(list)
        global_ids = set()
        
        # Captura cookies primeiro
        cookies_raw = self._get_cookies()
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR'
                )
                
                if cookies_raw:
                    context.add_cookies(cookies_raw)
                
                page = context.new_page()
                
                # Varre cada seção
                for url_path, table, display_name, extra_fields in self.sections:
                    print(f"\n📦 {display_name} → {table}")
                    
                    section_items = self._scrape_section(
                        page, url_path, table, display_name, extra_fields, global_ids
                    )
                    
                    items_by_table[table].extend(section_items)
                    self.stats['by_section'][url_path] = len(section_items)
                    self.stats['by_table'][table] += len(section_items)
                    
                    print(f"✅ {len(section_items)} itens → {table}")
                    
                    time.sleep(2)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
        
        self.stats['total_scraped'] = sum(len(items) for items in items_by_table.values())
        return items_by_table
    
    def _get_cookies(self) -> List[dict]:
        """Captura cookies do MegaLeilões"""
        print("  🍪 Capturando cookies...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR'
                )
                
                page = context.new_page()
                page.goto(self.base_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(3)
                
                cookies = context.cookies()
                browser.close()
                
                print(f"    ✅ {len(cookies)} cookies capturados")
                return cookies
                
        except Exception as e:
            print(f"    ⚠️ Erro ao capturar cookies: {e}")
            return []
    
    def _scrape_section(self, page, url_path: str, table: str, 
                       display_name: str, extra_fields: dict, 
                       global_ids: set) -> List[dict]:
        """Scrape uma seção específica"""
        items = []
        page_num = 1
        consecutive_empty = 0
        max_empty = 3
        max_pages = 50
        
        while page_num <= max_pages and consecutive_empty < max_empty:
            # Monta URL
            if page_num == 1:
                url = f"{self.base_url}/{url_path}"
            else:
                url = f"{self.base_url}/{url_path}?pagina={page_num}"
            
            print(f"  Pág {page_num}", end='', flush=True)
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(random.uniform(3, 5))
                
                # Scroll para carregar conteúdo lazy
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Seletor de cards
                cards = soup.select('div.card, .leilao-card, div[class*="card"]')
                
                if not cards:
                    print(f" ⚪ Sem cards")
                    consecutive_empty += 1
                    page_num += 1
                    continue
                
                novos = 0
                duplicados = 0
                
                for card in cards:
                    item = self._extract_card(card, table, display_name, extra_fields)
                    
                    if not item:
                        continue
                    
                    # Verifica duplicata
                    if item['external_id'] in global_ids:
                        duplicados += 1
                        self.stats['duplicates'] += 1
                        continue
                    
                    items.append(item)
                    global_ids.add(item['external_id'])
                    novos += 1
                
                if novos > 0:
                    print(f" ✅ +{novos}")
                    consecutive_empty = 0
                else:
                    print(f" ⚪ 0 novos (dup: {duplicados})")
                    consecutive_empty += 1
                
                page_num += 1
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                print(f" ❌ Erro: {str(e)[:80]}")
                consecutive_empty += 1
                page_num += 1
        
        return items
    
    def _extract_card(self, card, table: str, display_name: str, 
                     extra_fields: dict) -> Optional[dict]:
        """Extrai dados do card - dados limpos, sem decidir categoria"""
        try:
            # Link
            link_elem = card.select_one('a[href]')
            if not link_elem:
                return None
            
            link = link_elem.get('href', '')
            if not link or 'javascript' in link:
                return None
            
            # Normaliza link
            if not link.startswith('http'):
                link = f"{self.base_url}{link}"
            
            # Valida se é link de item (não de listagem)
            link_clean = link.rstrip('/')
            
            # Evita links de categorias/listagens
            invalid_endings = [
                '/imoveis', '/veiculos', '/bens-de-consumo', '/industrial', 
                '/animais', '/outros', '/carros', '/motos', '/apartamentos'
            ]
            
            if any(link_clean.endswith(ending) for ending in invalid_endings):
                return None
            
            # Extrai ID do item
            external_id = None
            parts = link.rstrip('/').split('/')
            for part in reversed(parts):
                if part and not part.startswith('?'):
                    external_id = f"megaleiloes_{part.split('?')[0]}"
                    break
            
            if not external_id or external_id == 'megaleiloes_':
                return None
            
            # Texto do card
            texto = card.get_text(separator=' ', strip=True)
            
            # Filtra cards de UI/paginação
            texto_lower = texto.lower()
            if 'exibindo' in texto_lower and 'itens' in texto_lower:
                return None
            
            if len(texto) < 10:
                return None
            
            # Título (primeiros 150 chars)
            title = texto[:150].strip() if texto else f"Item {display_name}"
            
            # ✅ EXTRAI INFORMAÇÕES DE PRAÇA DO HTML
            auction_info = self._extract_auction_info_from_html(card)
            
            # Preço (usa preço da praça ativa se disponível)
            value = auction_info.get('current_value')
            value_text = auction_info.get('current_value_text')
            
            # Fallback para preço no texto se não encontrou nas divs
            if not value:
                price_match = re.search(r'R\$\s*([\d.]+,\d{2})', texto)
                if price_match:
                    value_text = f"R$ {price_match.group(1)}"
                    try:
                        value = float(price_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
            
            # Estado
            state = None
            state_match = re.search(r'\b([A-Z]{2})\b', texto)
            if state_match:
                uf = state_match.group(1)
                valid_states = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
                               'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']
                if uf in valid_states:
                    state = uf
            
            # Cidade
            city = None
            city_match = re.search(r'([A-ZÀ-Ú][a-zà-ú\s]+)\s*[-–/,]\s*[A-Z]{2}', texto)
            if city_match:
                city = city_match.group(1).strip()
            
            # Monta item base
            item = {
                'source': 'megaleiloes',
                'external_id': external_id,
                'title': title,
                'description': texto,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                'target_table': table,  # ✅ Tabela de destino já definida
                
                # ✅ Informações de praça extraídas do HTML
                'auction_round': auction_info.get('auction_round'),
                'auction_date': auction_info.get('auction_date'),
                'first_round_value': auction_info.get('first_round_value'),
                'first_round_date': auction_info.get('first_round_date'),
                'discount_percentage': auction_info.get('discount_percentage'),
                
                'metadata': {
                    'secao_site': display_name,
                }
            }
            
            # Adiciona campos extras (vehicle_type, property_type, animal_type)
            if extra_fields:
                item.update(extra_fields)
            
            return item
            
        except Exception as e:
            return None
    
    def _extract_auction_info_from_html(self, card) -> dict:
        """
        ✅ Extrai informações de praça dos elementos HTML do card
        
        Busca por:
        - <div class="instance first passed"> → 1ª praça (já passou)
        - <div class="instance active"> → 2ª praça (ativa)
        - .card-first-instance-date → data da 1ª praça
        - .card-second-instance-date → data da 2ª praça
        - .card-instance-value → valores
        """
        info = {
            'auction_round': None,
            'auction_date': None,
            'current_value': None,
            'current_value_text': None,
            'first_round_value': None,
            'first_round_date': None,
            'discount_percentage': None,
        }
        
        # Busca praça ativa
        active_instance = card.select_one('.instance.active')
        
        if active_instance:
            # Determina qual praça está ativa
            if active_instance.select_one('.card-second-instance-date'):
                info['auction_round'] = 2
                
                # Data da 2ª praça
                date_elem = active_instance.select_one('.card-second-instance-date')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    # Extrai data: "2ª Praça: 07/01/2026 às 15:02"
                    date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                    if date_match:
                        info['auction_date'] = f"{date_match.group(1)} {date_match.group(2)}"
                
                # Valor da 2ª praça
                value_elem = active_instance.select_one('.card-instance-value')
                if value_elem:
                    value_text = value_elem.get_text(strip=True)
                    info['current_value_text'] = value_text
                    
                    # Parse valor
                    value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                    if value_match:
                        try:
                            info['current_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                        except:
                            pass
            
            elif active_instance.select_one('.card-first-instance-date'):
                info['auction_round'] = 1
                
                # Data da 1ª praça
                date_elem = active_instance.select_one('.card-first-instance-date')
                if date_elem:
                    date_text = date_elem.get_text(strip=True)
                    date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                    if date_match:
                        info['auction_date'] = f"{date_match.group(1)} {date_match.group(2)}"
                
                # Valor da 1ª praça
                value_elem = active_instance.select_one('.card-instance-value')
                if value_elem:
                    value_text = value_elem.get_text(strip=True)
                    info['current_value_text'] = value_text
                    
                    value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                    if value_match:
                        try:
                            info['current_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                        except:
                            pass
        
        # Busca 1ª praça (se já passou e agora está na 2ª)
        first_instance = card.select_one('.instance.first.passed')
        if first_instance:
            # Data da 1ª praça
            date_elem = first_instance.select_one('.card-first-instance-date')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    info['first_round_date'] = f"{date_match.group(1)} {date_match.group(2)}"
            
            # Valor da 1ª praça
            value_elem = first_instance.select_one('.card-instance-value')
            if value_elem:
                value_text = value_elem.get_text(strip=True)
                value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                if value_match:
                    try:
                        info['first_round_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
        
        # Calcula desconto se temos ambos os valores
        if info['first_round_value'] and info['current_value'] and info['auction_round'] == 2:
            try:
                discount = ((info['first_round_value'] - info['current_value']) / info['first_round_value']) * 100
                info['discount_percentage'] = round(discount, 2)
            except:
                pass
        
        return info


def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 MEGALEILÕES - SCRAPER SIMPLIFICADO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # ========================================
    # FASE 1: SCRAPE
    # ========================================
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = MegaLeiloesScraper()
    items_by_table = scraper.scrape()
    
    total_items = sum(len(items) for items in items_by_table.values())
    
    print(f"\n✅ Total coletado: {total_items} itens")
    print(f"📄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not total_items:
        print("⚠️ Nenhum item coletado - encerrando")
        return
    
    # ========================================
    # FASE 2: NORMALIZAÇÃO
    # ========================================
    print("\n✨ FASE 2: NORMALIZANDO DADOS")
    
    normalized_by_table = {}
    
    for table, items in items_by_table.items():
        if not items:
            continue
        
        try:
            normalized = normalize_items(items)
            normalized_by_table[table] = normalized
            print(f"  ✅ {table}: {len(normalized)} itens normalizados")
        except Exception as e:
            print(f"  ⚠️ Erro em {table}: {e}")
            normalized_by_table[table] = items
    
    # Salva JSON normalizado (debug)
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'megaleiloes_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_by_table, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo: {json_file}")
    
    # ========================================
    # FASE 3: INSERT NO SUPABASE
    # ========================================
    print("\n📤 FASE 3: INSERINDO NO SUPABASE")
    
    try:
        supabase = SupabaseClient()
        
        if not supabase.test():
            print("⚠️ Erro na conexão com Supabase - pulando insert")
        else:
            total_inserted = 0
            total_updated = 0
            
            for table, items in normalized_by_table.items():
                if not items:
                    continue
                
                print(f"\n  📤 Tabela '{table}': {len(items)} itens")
                stats = supabase.upsert(table, items)
                
                print(f"    ✅ Inseridos: {stats['inserted']}")
                print(f"    🔄 Atualizados: {stats['updated']}")
                if stats['errors'] > 0:
                    print(f"    ⚠️ Erros: {stats['errors']}")
                
                total_inserted += stats['inserted']
                total_updated += stats['updated']
            
            print(f"\n  📈 TOTAL:")
            print(f"    ✅ Inseridos: {total_inserted}")
            print(f"    🔄 Atualizados: {total_updated}")
    
    except Exception as e:
        print(f"⚠️ Erro no Supabase: {e}")
    
    # ========================================
    # ESTATÍSTICAS FINAIS
    # ========================================
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"🟢 MegaLeilões - Scraper Simplificado:")
    print(f"\n  Por Tabela:")
    for table, count in sorted(scraper.stats['by_table'].items()):
        print(f"    • {table}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()