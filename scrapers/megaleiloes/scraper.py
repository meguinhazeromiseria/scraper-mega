#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRAPER COMPLETO E CORRIGIDO
✅ Paginação automática detectando botão "Fim"
✅ Extrai data, lances e imagem corretamente
✅ Compatível 100% com tabela megaleiloes_items
✅ Usa ?pagina=N (não ?page=N)
"""

import sys
import json
import time
import re
import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Optional

from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

# Adiciona o diretório atual ao path para importar supabase_client
sys.path.insert(0, str(Path(__file__).parent))


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
    """Scraper para MegaLeilões com paginação automática"""
    
    def __init__(self):
        """Inicializa scraper"""
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        # Seções principais
        self.sections = [
            ('imoveis', 'Imóveis', 'Imóveis'),
            ('veiculos', 'Veículos', 'Veículos'),
            ('bens-de-consumo', 'Bens de Consumo', 'Bens de Consumo'),
            ('industrial', 'Industrial', 'Industrial'),
            ('animais', 'Animais', 'Animais'),
            ('outros', 'Outros', 'Outros'),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_category': {},
            'duplicates': 0,
            'with_bids': 0,
            'with_images': 0,
            'pages_scraped': 0,
        }
        
        # Estados brasileiros válidos
        self.valid_states = [
            'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
            'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
        ]
    
    def scrape(self) -> List[Dict]:
        """Scrape completo do MegaLeilões"""
        print("\n" + "="*70)
        print("🟢 MEGALEILÕES - SCRAPER COMPLETO")
        print("="*70)
        
        all_items = []
        global_ids = set()
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR'
                )
                
                page = context.new_page()
                
                for url_path, category, display_name in self.sections:
                    print(f"\n{'='*70}")
                    print(f"📦 {display_name}")
                    print(f"{'='*70}")
                    
                    section_items = self._scrape_section(
                        page, url_path, category, display_name, global_ids
                    )
                    
                    all_items.extend(section_items)
                    self.stats['by_category'][category] = len(section_items)
                    
                    print(f"✅ {len(section_items)} itens coletados de {display_name}")
                    
                    time.sleep(2)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            import traceback
            traceback.print_exc()
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _get_max_page(self, soup) -> int:
        """Detecta o número máximo de páginas pelo botão 'Fim'"""
        try:
            # Procura pelo botão "Fim" na paginação
            last_link = soup.select_one('ul.pagination li.last a')
            if last_link:
                href = last_link.get('href', '')
                # Extrai número da página do URL
                match = re.search(r'pagina=(\d+)', href)
                if match:
                    return int(match.group(1))
            
            # Se não encontrar, tenta pelos links de página
            page_links = soup.select('ul.pagination li a[data-page]')
            if page_links:
                pages = []
                for link in page_links:
                    href = link.get('href', '')
                    match = re.search(r'pagina=(\d+)', href)
                    if match:
                        pages.append(int(match.group(1)))
                if pages:
                    return max(pages)
            
            return 1
            
        except Exception:
            return 1
    
    def _scrape_section(self, page, url_path: str, category: str,
                       display_name: str, global_ids: set) -> List[Dict]:
        """Scrape uma seção específica - todas as páginas"""
        items = []
        
        # Primeiro acessa a página 1 para descobrir quantas páginas existem
        url = f"{self.base_url}/{url_path}"
        
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            
            # Detecta o número máximo de páginas
            max_page = self._get_max_page(soup)
            print(f"📄 Total de páginas detectadas: {max_page}")
            
            # Agora scrape todas as páginas
            for page_num in range(1, max_page + 1):
                if page_num == 1:
                    current_url = url
                    current_soup = soup
                else:
                    current_url = f"{url}?pagina={page_num}"
                    page.goto(current_url, wait_until="domcontentloaded", timeout=60000)
                    time.sleep(3)
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(2)
                    current_html = page.content()
                    current_soup = BeautifulSoup(current_html, 'html.parser')
                
                # Extrai cards
                cards = current_soup.select('div.card')
                
                if not cards:
                    print(f"  ⚠️ Página {page_num}/{max_page}: Nenhum card encontrado")
                    continue
                
                print(f"  📄 Página {page_num}/{max_page}: {len(cards)} cards encontrados")
                
                page_items = 0
                for card in cards:
                    item = self._parse_card(card, category)
                    
                    if item and item['external_id'] not in global_ids:
                        items.append(item)
                        global_ids.add(item['external_id'])
                        page_items += 1
                        
                        if item.get('has_bid'):
                            self.stats['with_bids'] += 1
                        
                        if item.get('image_url'):
                            self.stats['with_images'] += 1
                    elif item:
                        self.stats['duplicates'] += 1
                
                self.stats['pages_scraped'] += 1
                print(f"  ✅ {page_items} itens válidos extraídos da página {page_num}")
                
                # Delay entre páginas
                time.sleep(2)
        
        except Exception as e:
            print(f"❌ Erro ao processar seção: {e}")
            import traceback
            traceback.print_exc()
        
        return items
    
    def _parse_card(self, card, category: str) -> Optional[Dict]:
        """Parse de um card de leilão"""
        try:
            # 1. Extrai link
            link_elem = card.select_one('a[href]')
            if not link_elem:
                return None
            
            link = link_elem.get('href', '')
            if not link or 'javascript' in link.lower():
                return None
            
            if not link.startswith('http'):
                link = f"{self.base_url}{link}"
            
            # Remove parâmetros UTM
            link_clean = link.split('?')[0].rstrip('/')
            
            # 2. Extrai external_id do link
            external_id = None
            parts = link_clean.split('/')
            for part in reversed(parts):
                if part and not part.startswith('?'):
                    external_id = f"{self.source}_{part}"
                    break
            
            if not external_id or external_id == f'{self.source}_':
                return None
            
            # 3. Extrai texto completo
            texto = card.get_text(separator=' ', strip=True)
            
            # Filtra cards muito curtos
            if len(texto) < 20:
                return None
            
            # 4. Título (prioriza .card-title)
            title_elem = card.select_one('.card-title')
            if title_elem:
                title = title_elem.get_text(strip=True)
            else:
                # Pega primeiras palavras do texto
                words = texto.split()[:15]
                title = ' '.join(words)
            
            # 5. Imagem (data-bg do a.card-image)
            image_url = None
            image_elem = card.select_one('a.card-image[data-bg]')
            if image_elem:
                image_url = image_elem.get('data-bg')
                # Filtra imagem padrão "no-image"
                if image_url and 'no-image' in image_url:
                    image_url = None
            
            # 6. Extrai informações de praça
            auction_info = self._extract_auction_info_from_html(card)
            
            # 7. Has bid (ícone fa-legal)
            has_bid = self._extract_has_bid(card)
            
            # 8. Valor
            value = auction_info.get('current_value')
            value_text = auction_info.get('current_value_text')
            
            if not value:
                price_elem = card.select_one('.card-price')
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    price_match = re.search(r'R\$\s*([\d.]+,\d{2})', price_text)
                    if price_match:
                        value_text = f"R$ {price_match.group(1)}"
                        try:
                            value = float(price_match.group(1).replace('.', '').replace(',', '.'))
                        except:
                            pass
            
            # 9. Cidade e Estado (usa .card-locality se disponível)
            city = None
            state = None
            
            locality_elem = card.select_one('.card-locality')
            if locality_elem:
                locality_text = locality_elem.get_text(strip=True)
                # Formato: "São João Del Rei, MG"
                match = re.match(r'^(.+),\s*([A-Z]{2})$', locality_text)
                if match:
                    city = match.group(1).strip()
                    state = match.group(2).strip()
            
            # Se não encontrou, tenta no texto geral
            if not city or not state:
                city_match = re.search(r'([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*,\s*([A-Z]{2})\b', texto)
                if city_match:
                    if not city:
                        city = city_match.group(1).strip()
                    if not state:
                        state = city_match.group(2)
            
            # 10. Tipo de leilão (usa .card-instance-title a)
            auction_type = None
            type_elem = card.select_one('.card-instance-title a')
            if type_elem:
                type_text = type_elem.get_text(strip=True)
                if 'judicial' in type_text.lower():
                    auction_type = 'Judicial'
                elif 'extrajudicial' in type_text.lower():
                    auction_type = 'Extrajudicial'
            
            # Se não encontrou, busca no texto
            if not auction_type:
                if 'judicial' in texto.lower():
                    auction_type = 'Judicial'
                elif 'extrajudicial' in texto.lower():
                    auction_type = 'Extrajudicial'
            
            # 11. Número do lote (card-number)
            batch_number = None
            number_elem = card.select_one('.card-number')
            if number_elem:
                batch_number = number_elem.get_text(strip=True)
            
            # 12. Constrói o item compatível com DB
            item = {
                'source': self.source,
                'external_id': external_id,
                'category': category,
                'title': title,
                'description': texto,
                'city': city,
                'state': state,
                'value': value,
                'value_text': value_text,
                'auction_round': auction_info.get('auction_round'),
                'auction_date': auction_info.get('auction_date'),
                'first_round_value': auction_info.get('first_round_value'),
                'first_round_date': auction_info.get('first_round_date'),
                'discount_percentage': auction_info.get('discount_percentage'),
                'link': link,
                'image_url': image_url,
                'metadata': {'batch_number': batch_number} if batch_number else {},
                'is_active': True,
                'has_bid': has_bid,
                'auction_type': auction_type,
            }
            
            return item
            
        except Exception:
            return None
    
    def _extract_has_bid(self, card) -> bool:
        """Verifica se o item tem lances - procura pelo ícone fa-legal"""
        try:
            legal_icon = card.select_one('i.fa-legal')
            
            if legal_icon:
                parent_span = legal_icon.find_parent('span')
                if parent_span:
                    text = parent_span.get_text(strip=True)
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
        
        # Praça ativa (atual)
        active_instance = card.select_one('.instance.active')
        
        if active_instance:
            # Verifica se é segunda praça
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
            
            # Valor atual
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
        
        # Primeira praça (histórico)
        first_instance = card.select_one('.instance.first.passed')
        if first_instance:
            # Data da primeira praça
            date_elem = first_instance.select_one('.card-first-instance-date')
            if date_elem:
                date_text = date_elem.get_text(strip=True)
                date_match = re.search(r'(\d{2}/\d{2}/\d{4})\s*às\s*(\d{2}:\d{2})', date_text)
                if date_match:
                    date_str = f"{date_match.group(1)} {date_match.group(2)}"
                    info['first_round_date'] = convert_brazilian_datetime_to_postgres(date_str)
            
            # Valor da primeira praça
            value_elem = first_instance.select_one('.card-instance-value')
            if value_elem:
                value_text = value_elem.get_text(strip=True)
                value_match = re.search(r'R\$\s*([\d.]+,\d{2})', value_text)
                if value_match:
                    try:
                        info['first_round_value'] = float(value_match.group(1).replace('.', '').replace(',', '.'))
                    except:
                        pass
        
        # Calcula desconto (se for segunda praça)
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
    print("🚀 MEGALEILÕES - SCRAPER COMPLETO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # Scrape
    scraper = MegaLeiloesScraper()
    items = scraper.scrape()
    
    print(f"\n{'='*70}")
    print(f"📊 RESULTADO FINAL")
    print(f"{'='*70}")
    print(f"✅ Total coletado: {len(items)} itens")
    print(f"📄 Páginas processadas: {scraper.stats['pages_scraped']}")
    print(f"🖼️ Itens com imagens: {scraper.stats['with_images']}")
    print(f"🔥 Itens com lances: {scraper.stats['with_bids']}")
    print(f"🔄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not items:
        print("\n⚠️ Nenhum item coletado - encerrando")
        return
    
    # Salva JSON
    output_dir = Path(__file__).parent / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'megaleiloes_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    print(f"\n💾 JSON salvo: {json_file}")
    
    # Importa e usa o cliente Supabase
    try:
        # Verifica se as variáveis de ambiente estão configuradas
        if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_SERVICE_ROLE_KEY'):
            print("\n⚠️ Variáveis SUPABASE não configuradas - pulando insert")
        else:
            from supabase_client import SupabaseMegaLeiloes
            
            print(f"\n{'='*70}")
            print("📤 INSERINDO NO SUPABASE")
            print(f"{'='*70}")
            
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
    
    except ImportError as e:
        print(f"\n⚠️ Módulo supabase_client não encontrado: {e}")
        print("   (JSON salvo, mas não foi possível inserir no banco)")
    except Exception as e:
        print(f"\n⚠️ Erro no Supabase: {e}")
        import traceback
        traceback.print_exc()
    
    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)
    
    print(f"\n{'='*70}")
    print("📊 ESTATÍSTICAS FINAIS")
    print(f"{'='*70}")
    print(f"\n  Por Categoria:")
    for category, count in sorted(scraper.stats['by_category'].items()):
        print(f"    • {category}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Páginas processadas: {scraper.stats['pages_scraped']}")
    print(f"  • Com imagens: {scraper.stats['with_images']}")
    print(f"  • Com lances: {scraper.stats['with_bids']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()