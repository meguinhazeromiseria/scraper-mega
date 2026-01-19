#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRAPER DIRETO PARA megaleiloes_items
✅ Remove dependência do normalizer
✅ Salva diretamente na tabela megaleiloes_items
✅ Extrai has_bid do HTML
✅ Mantém informações de praça (auction_round, discount_percentage, etc)
"""

import sys
import json
import time
import re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional

# Imports do scraper
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


def convert_brazilian_datetime_to_postgres(date_str: str) -> Optional[str]:
    """Converte data brasileira DD/MM/YYYY HH:MM para PostgreSQL ISO format"""
    try:
        date_str = date_str.replace('às', '').strip()
        dt = datetime.strptime(date_str, '%d/%m/%Y %H:%M')
        dt_with_tz = dt.replace(tzinfo=ZoneInfo('America/Sao_Paulo'))
        return dt_with_tz.isoformat()
    except Exception:
        return None


class MegaLeiloesScraper:
    """Scraper para MegaLeilões com mapeamento de categorias"""
    
    def __init__(self):
        """Inicializa scraper - coleta TODAS as páginas disponíveis"""
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        # Mapeamento: (url_path, category, display_name)
        self.sections = [
            # VEÍCULOS
            ('veiculos/aeronaves', 'Aeronaves', 'Aeronaves'),
            ('veiculos/barcos', 'Barcos', 'Barcos'),
            ('veiculos/caminhoes', 'Caminhões', 'Caminhões'),
            ('veiculos/carros', 'Carros', 'Carros'),
            ('veiculos/motos', 'Motos', 'Motos'),
            ('veiculos/onibus', 'Ônibus', 'Ônibus'),
            
            # IMÓVEIS
            ('imoveis/apartamentos', 'Apartamentos', 'Apartamentos'),
            ('imoveis/casas', 'Casas', 'Casas'),
            ('imoveis/galpoes--industriais', 'Galpões Industriais', 'Galpões Industriais'),
            ('imoveis/glebas', 'Glebas', 'Glebas'),
            ('imoveis/deposito-de-garagem', 'Depósito de Garagem', 'Depósito de Garagem'),
            ('imoveis/hospitais', 'Hospitais', 'Hospitais'),
            ('imoveis/hoteis', 'Hotéis', 'Hotéis'),
            ('imoveis/imoveis-comerciais', 'Imóveis Comerciais', 'Imóveis Comerciais'),
            ('imoveis/imoveis-rurais', 'Imóveis Rurais', 'Imóveis Rurais'),
            ('imoveis/outros', 'Outros Imóveis', 'Outros Imóveis'),
            ('imoveis/resorts', 'Resorts', 'Resorts'),
            ('imoveis/terrenos-e-lotes', 'Terrenos e Lotes', 'Terrenos e Lotes'),
            ('imoveis/terrenos-para-incorporacao', 'Terrenos p/ Incorporação', 'Terrenos p/ Incorporação'),
            ('imoveis/vagas-de-garagem', 'Vagas de Garagem', 'Vagas de Garagem'),
            
            # OUTROS
            ('bens-de-consumo/eletrodomesticos', 'Eletrodomésticos', 'Eletrodomésticos'),
            ('bens-de-consumo/eletronicos', 'Eletrônicos', 'Eletrônicos'),
            ('bens-de-consumo/moveis', 'Móveis', 'Móveis'),
            ('industrial/maquinas', 'Máquinas Industriais', 'Máquinas Industriais'),
            ('animais/cavalos', 'Cavalos', 'Cavalos'),
            ('animais/gado-bovino', 'Gado Bovino', 'Gado Bovino'),
            ('outros/diversos', 'Diversos', 'Diversos'),
            ('outros/obras-de-arte', 'Obras de Arte', 'Obras de Arte'),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_category': {},
            'duplicates': 0,
            'with_bids': 0,
        }
    
    def scrape(self) -> List[Dict]:
        """Scrape completo do MegaLeilões - retorna lista de itens"""
        print("\n" + "="*60)
        print("🟢 MEGALEILÕES - SCRAPER")
        print("="*60)
        
        all_items = []
        global_ids = set()
        
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
                
                for url_path, category, display_name in self.sections:
                    print(f"\n📦 {display_name}")
                    
                    section_items = self._scrape_section(
                        page, url_path, category, display_name, global_ids
                    )
                    
                    all_items.extend(section_items)
                    self.stats['by_category'][category] = len(section_items)
                    
                    print(f"✅ {len(section_items)} itens")
                    
                    time.sleep(2)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
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
    
    def _scrape_section(self, page, url_path: str, category: str,
                       display_name: str, global_ids: set) -> List[Dict]:
        """Scrape uma seção específica - TODAS as páginas disponíveis"""
        items = []
        page_num = 1
        consecutive_empty = 0
        max_empty = 3  # Para páginas vazias consecutivas antes de parar
        
        while True:  # ✅ SEM LIMITE! Vai até acabar as páginas
            url = f"{self.base_url}/{url_path}?page={page_num}"
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                cards = soup.select('div.card')
                
                if not cards:
                    consecutive_empty += 1
                    print(f"    ⚠️ Página {page_num} vazia ({consecutive_empty}/{max_empty})")
                    if consecutive_empty >= max_empty:
                        print(f"    ✅ Fim da categoria (sem mais itens)")
                        break
                    page_num += 1
                    continue
                
                consecutive_empty = 0
                print(f"    📄 Página {page_num}: {len(cards)} cards encontrados")
                
                for card in cards:
                    item = self._parse_card(card, category)
                    
                    if item and item['external_id'] not in global_ids:
                        items.append(item)
                        global_ids.add(item['external_id'])
                        
                        if item.get('has_bid'):
                            self.stats['with_bids'] += 1
                    elif item:
                        self.stats['duplicates'] += 1
                
                page_num += 1
                
            except Exception as e:
                print(f"    ⚠️ Erro na página {page_num}: {e}")
                page_num += 1
                continue
        
        return items
    
    def _parse_card(self, card, category: str) -> Optional[Dict]:
        """Parse de um card de leilão - formato megaleiloes_items"""
        try:
            # Link e external_id
            link_elem = card.select_one('a.card-link')
            if not link_elem or not link_elem.get('href'):
                return None
            
            link = link_elem['href']
            if not link.startswith('http'):
                link = self.base_url + link
            
            # External ID da URL
            external_id = f"{self.source}_{link.split('/')[-1].split('?')[0]}"
            
            # Título
            title_elem = card.select_one('.card-title')
            title = title_elem.get_text(strip=True) if title_elem else 'Sem Título'
            
            # Descrição (todo o texto do card)
            description = card.get_text(separator=' ', strip=True)
            
            # Localização
            location_elem = card.select_one('.card-location')
            city = None
            state = None
            
            if location_elem:
                location_text = location_elem.get_text(strip=True)
                if ',' in location_text:
                    parts = location_text.split(',')
                    city = parts[0].strip()
                    state_raw = parts[1].strip()
                    if len(state_raw) == 2:
                        state = state_raw.upper()
            
            # Informações de praça
            auction_info = self._extract_auction_info_from_html(card)
            
            # Has bid
            has_bid = self._extract_has_bid(card)
            
            # Tipo de leilão
            auction_type_elem = card.select_one('.card-auction-type')
            auction_type = auction_type_elem.get_text(strip=True) if auction_type_elem else None
            
            # Constrói o item no formato da tabela megaleiloes_items
            item = {
                'source': self.source,
                'external_id': external_id,
                'category': category,
                'title': title,
                'description': description,
                'city': city,
                'state': state,
                'value': auction_info.get('current_value'),
                'value_text': auction_info.get('current_value_text'),
                'auction_round': auction_info.get('auction_round'),
                'auction_date': auction_info.get('auction_date'),
                'first_round_value': auction_info.get('first_round_value'),
                'first_round_date': auction_info.get('first_round_date'),
                'discount_percentage': auction_info.get('discount_percentage'),
                'link': link,
                'metadata': {},
                'is_active': True,
                'has_bid': has_bid,
                'auction_type': auction_type,
            }
            
            return item
            
        except Exception as e:
            return None
    
    def _extract_has_bid(self, card) -> bool:
        """Verifica se o item tem lances"""
        try:
            bid_info = card.select_one('.card-bid-info')
            if bid_info:
                spans = bid_info.select('span')
                for span in spans:
                    if 'lance' in span.get_text(strip=True).lower():
                        text = span.get_text(strip=True)
                        numbers = re.findall(r'\d+', text)
                        if numbers:
                            bid_count = int(numbers[0])
                            return bid_count > 0
            
            return False
            
        except Exception:
            return False
    
    def _extract_auction_info_from_html(self, card) -> Dict:
        """Extrai informações de praça do HTML"""
        info = {
            'auction_round': None,
            'auction_date': None,
            'current_value': None,
            'current_value_text': None,
            'first_round_value': None,
            'first_round_date': None,
            'discount_percentage': None,
        }
        
        # Praça ativa
        active_instance = card.select_one('.instance.active')
        
        if active_instance:
            # Verifica se é 2ª praça
            second_date = active_instance.select_one('.card-second-instance-date')
            first_date = active_instance.select_one('.card-first-instance-date')
            
            if second_date:
                info['auction_round'] = 2
                date_text = second_date.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['auction_date'] = convert_brazilian_datetime_to_postgres(date_str)
                
            elif first_date:
                info['auction_round'] = 1
                date_text = first_date.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['auction_date'] = convert_brazilian_datetime_to_postgres(date_str)
            
            # Valor da praça ativa
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
        
        # Primeira praça (se passou)
        first_instance = card.select_one('.instance.first.passed')
        if first_instance:
            date_elem = first_instance.select_one('.card-first-instance-date')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['first_round_date'] = convert_brazilian_datetime_to_postgres(date_str)
            
            value_elem = first_instance.select_one('.card-instance-value')
            if value_elem:
                value_text = value_elem.get_text(strip=True)
                value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                if value_match:
                    try:
                        info['first_round_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
        
        # Calcula percentual de desconto
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
    print("🚀 MEGALEILÕES - SCRAPER")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # Scrape
    scraper = MegaLeiloesScraper()
    items = scraper.scrape()
    
    print(f"\n✅ Total coletado: {len(items)} itens")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not items:
        print("⚠️ Nenhum item coletado - encerrando")
        return
    
    # Salva JSON
    output_dir = Path(__file__).parent / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'megaleiloes_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo: {json_file}")
    
    # Importa e usa o cliente Supabase
    try:
        from supabase_megaleiloes import SupabaseMegaLeiloes
        
        print("\n📤 INSERINDO NO SUPABASE")
        supabase = SupabaseMegaLeiloes()
        
        if not supabase.test():
            print("⚠️ Erro na conexão com Supabase - pulando insert")
        else:
            stats = supabase.upsert(items)
            
            print(f"\n  📈 RESULTADO:")
            print(f"    ✅ Inseridos: {stats['inserted']}")
            print(f"    🔄 Atualizados: {stats['updated']}")
            if stats['errors'] > 0:
                print(f"    ⚠️ Erros: {stats['errors']}")
    
    except Exception as e:
        print(f"⚠️ Erro no Supabase: {e}")
    
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print("\n" + "="*70)
    print("📊 ESTATÍSTICAS FINAIS")
    print("="*70)
    print(f"\n  Por Categoria:")
    for category, count in sorted(scraper.stats['by_category'].items()):
        print(f"    • {category}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Com lances: {scraper.stats['with_bids']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()