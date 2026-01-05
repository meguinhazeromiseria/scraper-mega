#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SODRÉ SANTORO - SCRAPER COMPLETO DO DOMÍNIO
Varre TODO o site Sodré Santoro (veículos, materiais, imóveis, sucatas)
Scrape → Normalize → Classify → Insert
"""

import sys
import json
import time
import random
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Optional

# Adiciona pasta pai ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from groq_classifier import GroqTableClassifier
from normalizer import normalize_items

# Imports do scraper
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


class SodreScraper:
    """Scraper completo do domínio Sodré Santoro"""
    
    def __init__(self):
        self.source = 'sodre'
        self.base_url = 'https://www.sodresantoro.com.br'
        
        # Seções principais do Sodré Santoro
        # Para veículos: subcategorias específicas com vehicle_type
        # Para outras: categoria principal
        self.main_sections = [
            # VEÍCULOS (subcategorias específicas)
            ('veiculos', 'carros', 'Carros', {'vehicle_type': 'carro'}),
            ('veiculos', 'utilitarios+leves', 'Utilitários Leves', {'vehicle_type': 'carro'}),
            ('veiculos', 'motos', 'Motos', {'vehicle_type': 'moto'}),
            ('veiculos', 'onibus', 'Ônibus', {'vehicle_type': 'onibus'}),
            ('veiculos', 'peruas', 'Peruas', {'vehicle_type': 'perua'}),
            ('veiculos', 'utilit.+pesados', 'Utilitários Pesados', {'vehicle_type': 'pesados'}),
            ('veiculos', 'van+leve', 'Van Leve', {'vehicle_type': 'van'}),
            ('veiculos', 'caminh%C3%B5es', 'Caminhões', {'vehicle_type': 'caminhao'}),
            
            # OUTRAS CATEGORIAS (sem subcategoria e sem vehicle_type)
            ('materiais', None, 'Materiais', {}),
            ('imoveis', None, 'Imóveis', {}),
            ('sucatas', None, 'Sucatas', {}),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_section': {},
            'duplicates': 0,
        }
    
    def scrape(self) -> List[dict]:
        """Scrape completo do Sodré Santoro"""
        print("\n" + "="*60)
        print("🟣 SODRÉ SANTORO - DOMÍNIO COMPLETO")
        print("="*60)
        
        all_items = []
        global_ids = set()
        
        try:
            with sync_playwright() as p:
                print("  🌐 Abrindo navegador...")
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled', '--disable-dev-shm-usage', '--no-sandbox']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR',
                    timezone_id='America/Sao_Paulo',
                )
                
                page = context.new_page()
                
                # Varre cada seção
                for main_category, subcategory, display_name, extra_fields in self.main_sections:
                    print(f"\n📦 Seção: {display_name}")
                    
                    section_key = f"{main_category}/{subcategory}" if subcategory else main_category
                    section_items = self._scrape_section(
                        page, main_category, subcategory, display_name, extra_fields, global_ids
                    )
                    
                    all_items.extend(section_items)
                    self.stats['by_section'][section_key] = len(section_items)
                    
                    print(f"✅ {len(section_items)} itens coletados")
                    
                    # Delay entre seções
                    time.sleep(3)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _scrape_section(self, page, main_category: str, subcategory: Optional[str], 
                       display_name: str, extra_fields: dict, global_ids: set) -> List[dict]:
        """Scrape uma seção completa do Sodré Santoro"""
        items = []
        
        # Monta URL base
        if subcategory:
            # Veículos com filtro de subcategoria
            url = f"{self.base_url}/{main_category}/lotes?lot_category={subcategory}&sort=auction_date_init_asc"
        else:
            # Outras categorias sem filtro
            url = f"{self.base_url}/{main_category}/lotes?sort=auction_date_init_asc"
        
        print(f"  🔗 URL: {url}")
        
        try:
            page.goto(url, wait_until="networkidle", timeout=60000)
            time.sleep(5)
        except Exception as e:
            print(f"  ❌ Erro ao carregar: {e}")
            return items
        
        page_num = 1
        sem_novos = 0
        max_sem_novos = 3
        max_pages = 50
        
        while page_num <= max_pages and sem_novos < max_sem_novos:
            try:
                # Scroll para carregar conteúdo lazy
                for i in range(3):
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(2)
                page.evaluate("window.scrollTo(0, 0)")
                time.sleep(1)
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Seletor de cards
                cards = soup.select('a[href*="leilao.sodresantoro.com.br/leilao/"]')
                
                if not cards:
                    print(f"  Pág {page_num} ⚪ Sem cards")
                    sem_novos += 1
                    if not self._try_next_page(page):
                        break
                    page_num += 1
                    continue
                
                novos = 0
                duplicados = 0
                
                for card in cards:
                    item = self._extract_card(card, main_category, subcategory, display_name, extra_fields)
                    
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
                    print(f"  Pág {page_num} ✅ +{novos} | Total seção: {len(items)}")
                    sem_novos = 0
                else:
                    print(f"  Pág {page_num} ⚪ 0 novos (dup: {duplicados})")
                    sem_novos += 1
                
                if not self._try_next_page(page):
                    break
                    
                page_num += 1
                
            except Exception as e:
                print(f"  ⚠️ Erro página {page_num}: {str(e)[:80]}")
                sem_novos += 1
                page_num += 1
        
        return items
    
    def _try_next_page(self, page) -> bool:
        """Tenta ir para próxima página"""
        try:
            time.sleep(2)
            
            # Seletores possíveis para botão "Avançar"
            selectors = [
                'button[title="Avançar"]:not([disabled])',
                'nav[aria-label*="Paginação"] button:has(.i-mdi\\:chevron-right):not([disabled])',
                'button:has(.iconify.i-mdi\\:chevron-right):not([disabled])',
            ]
            
            next_btn = None
            
            for selector in selectors:
                try:
                    next_btn = page.query_selector(selector)
                    if next_btn:
                        is_visible = next_btn.is_visible()
                        is_enabled = next_btn.is_enabled()
                        has_disabled = page.evaluate('(btn) => btn.hasAttribute("disabled")', next_btn)
                        
                        if is_visible and is_enabled and not has_disabled:
                            break
                        else:
                            next_btn = None
                except:
                    continue
            
            if not next_btn:
                return False
            
            try:
                next_btn.scroll_into_view_if_needed()
                time.sleep(1)
            except:
                pass
            
            try:
                next_btn.click(timeout=10000)
                time.sleep(random.uniform(4, 6))
                return True
            except:
                return False
                
        except:
            return False
    
    def _extract_card(self, card, main_category: str, subcategory: Optional[str],
                     display_name: str, extra_fields: dict) -> Optional[dict]:
        """
        Extrai dados do card Sodré Santoro.
        NÃO decide categoria final - apenas coleta dados brutos.
        Mantém vehicle_type quando disponível (veículos).
        """
        try:
            # Link
            link = card.get('href', '')
            if not link or 'javascript' in link:
                return None
            
            if not link.startswith('http'):
                link = f"{self.base_url}{link}"
            
            # Extrai IDs
            lot_match = re.search(r'/lote/(\d+)', link)
            auction_match = re.search(r'/leilao/(\d+)', link)
            
            if not lot_match:
                return None
            
            lot_id = lot_match.group(1)
            auction_id = auction_match.group(1) if auction_match else None
            external_id = f"sodre_{lot_id}"
            
            # Título
            title = None
            title_div = card.select_one('.text-body-medium.text-on-surface')
            if title_div:
                title_span = title_div.find('span')
                if title_span:
                    title = title_span.get_text(strip=True)
            
            if not title:
                title = card.get('title', '')
            
            if not title or len(title) < 3:
                return None
            
            # Preço
            value = None
            value_text = None
            price_elem = card.select_one('.text-primary.text-headline-small')
            if price_elem:
                price_str = price_elem.get_text(strip=True)
                price_clean = re.sub(r'[^\d,]', '', price_str)
                if price_clean:
                    value_text = f"R$ {price_clean}"
                    try:
                        value = float(price_clean.replace('.', '').replace(',', '.'))
                    except:
                        pass
            
            # Localização
            city = None
            state = None
            location_items = card.select('li')
            for li in location_items:
                icon = li.select_one('.iconify')
                if icon and 'location-on' in icon.get('class', []):
                    location_text = li.get_text(strip=True)
                    if '/' in location_text:
                        parts = location_text.split('/')
                        city = parts[0].strip()
                        state = parts[1].strip()
                    break
            
            # Leilão e Lote
            auction_name = None
            lot_number = None
            auction_elem = card.select_one('.text-label-small')
            if auction_elem:
                auction_text = auction_elem.get_text(strip=True)
                match = re.search(r'Leilão\s+(\d+)\s*-\s*(\d+)', auction_text, re.IGNORECASE)
                if match:
                    auction_name = f"Leilão {match.group(1)}"
                    lot_number = match.group(2)
            
            # Leiloeiro/vendedor
            store_name = None
            seller_divs = card.select('.text-body-small.text-on-surface-variant.uppercase')
            if seller_divs and len(seller_divs) > 0:
                store_name = seller_divs[0].get_text(strip=True)
            
            # Data do leilão
            auction_date = None
            date_elem = card.select_one('.i-mdi\\:calendar')
            if date_elem:
                parent = date_elem.parent
                if parent:
                    date_span = parent.select_one('.text-body-small')
                    if date_span:
                        date_text = date_span.get_text(strip=True)
                        try:
                            date_parts = date_text.split()
                            if len(date_parts) >= 2:
                                date_str = date_parts[0]
                                time_str = date_parts[1]
                                day, month, year = date_str.split('/')
                                year = f"20{year}" if len(year) == 2 else year
                                datetime_str = f"{year}-{month}-{day} {time_str}"
                                auction_date = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')
                        except:
                            pass
            
            # Visualizações
            total_visits = 0
            visibility_icon = card.select_one('.i-material-symbols\\:visibility-rounded')
            if visibility_icon:
                next_sibling = visibility_icon.find_next_sibling()
                if next_sibling:
                    visits_text = next_sibling.get_text(strip=True)
                    try:
                        total_visits = int(visits_text)
                    except:
                        pass
            
            # Categoria do item (do próprio card)
            item_category = None
            first_li = card.select_one('li')
            if first_li:
                item_category = first_li.get_text(strip=True)
            
            # Tipo de sinistro (para veículos)
            damage_type = None
            damage_items = card.select('li')
            for li in damage_items:
                icon = li.select_one('.iconify')
                if icon and 'car-crash' in icon.get('class', []):
                    damage_type = li.get_text(strip=True)
                    break
            
            # Descrição (texto completo do card)
            description = card.get_text(separator=' ', strip=True)
            
            # ✅ MONTA ITEM BASE - SEM DECIDIR CATEGORIA
            section_key = f"{main_category}/{subcategory}" if subcategory else main_category
            
            item = {
                'source': 'sodre',
                'external_id': external_id,
                'title': title,
                'description': description,
                'description_preview': description[:200] if len(description) > 200 else description,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                
                # Categoria ORIGINAL do site (só metadata, não decisão)
                'raw_category': section_key,
                
                'metadata': {
                    'secao_site': display_name,
                    'categoria_principal': main_category,
                    'subcategoria': subcategory,
                    'leilao_id': auction_id,
                    'leilao_nome': auction_name,
                    'lote_numero': lot_number,
                    'loja_nome': store_name,
                    'categoria_item': item_category,
                    'tipo_sinistro': damage_type,
                    'total_visitas': total_visits,
                    'data_leilao': auction_date.isoformat() if auction_date else None,
                }
            }
            
            # ✅ ADICIONA VEHICLE_TYPE APENAS PARA VEÍCULOS
            # Isso ajuda os handlers de busca, mas NÃO define a tabela final
            if 'vehicle_type' in extra_fields:
                item['vehicle_type'] = extra_fields['vehicle_type']
            
            return item
            
        except Exception as e:
            # Silencioso - não loga cada erro de parsing
            return None


def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SODRÉ SANTORO - SCRAPER COMPLETO DO DOMÍNIO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # ========================================
    # FASE 1: SCRAPE
    # ========================================
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SodreScraper()
    items = scraper.scrape()
    
    print(f"\n✅ Total coletado: {len(items)} itens")
    print(f"🔄 Duplicatas filtradas: {scraper.stats['duplicates']}")
    
    if not items:
        print("⚠️ Nenhum item coletado - encerrando")
        return
    
    # ========================================
    # FASE 2: NORMALIZAÇÃO
    # ========================================
    print("\n✨ FASE 2: NORMALIZANDO DADOS")
    try:
        normalized_items = normalize_items(items)
        print(f"✅ {len(normalized_items)} itens normalizados")
    except Exception as e:
        print(f"⚠️ Erro na normalização: {e}")
        print("Usando dados brutos...")
        normalized_items = items
    
    # Salva JSON normalizado (para debug)
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'sodre_{timestamp}.json'
    
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(normalized_items, f, ensure_ascii=False, indent=2)
    print(f"💾 JSON salvo: {json_file}")
    
    # ========================================
    # FASE 3: CLASSIFICAÇÃO GROQ
    # ========================================
    print("\n🤖 FASE 3: CLASSIFICANDO COM GROQ AI")
    try:
        classifier = GroqTableClassifier()
        items_by_table = defaultdict(list)
        
        for i, item in enumerate(normalized_items, 1):
            if i % 10 == 0:
                print(f"  ⏳ {i}/{len(normalized_items)}")
            
            table = classifier.classify(item)
            if table:
                items_by_table[table].append(item)
            
            time.sleep(0.2)  # Rate limit Groq
        
        print(f"✅ Classificação concluída!")
        print(f"\n📊 Distribuição por tabela:")
        for table, table_items in sorted(items_by_table.items()):
            print(f"  • {table}: {len(table_items)} itens")
        
        # Print stats do classifier
        classifier.print_stats()
    
    except Exception as e:
        print(f"⚠️ Erro na classificação: {e}")
        print("Colocando tudo em 'oportunidades'...")
        items_by_table = {'oportunidades': normalized_items}
    
    # ========================================
    # FASE 4: INSERT NO SUPABASE
    # ========================================
    print("\n📤 FASE 4: INSERINDO NO SUPABASE")
    try:
        supabase = SupabaseClient()
        
        if not supabase.test():
            print("⚠️ Erro na conexão com Supabase - pulando insert")
        else:
            total_inserted = 0
            total_updated = 0
            
            for table, table_items in items_by_table.items():
                if not table_items:
                    continue
                
                print(f"\n  📤 Tabela '{table}': {len(table_items)} itens")
                stats = supabase.upsert(table, table_items)
                
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
    print(f"🟣 Sodré Santoro - Domínio Completo:")
    print(f"\n  Por Seção do Site:")
    for section, count in sorted(scraper.stats['by_section'].items()):
        print(f"    • {section}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()