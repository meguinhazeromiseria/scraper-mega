#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MEGALEILÕES - SCRAPER COMPLETO DO DOMÍNIO
Varre TODO o site MegaLeilões (imóveis, veículos, bens, industrial, animais, outros)
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

# Adiciona pasta scrapers/ ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from groq_classifier import GroqTableClassifier
from normalizer import normalize_items

# Imports do scraper
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup


class MegaLeiloesScraper:
    """Scraper completo do domínio MegaLeilões"""
    
    def __init__(self):
        self.source = 'megaleiloes'
        self.base_url = 'https://www.megaleiloes.com.br'
        
        # Seções principais do site
        # Para veículos: subcategorias específicas com vehicle_type
        # Para outras: categoria principal
        self.main_sections = [
            # VEÍCULOS (6 subcategorias - COMPLETO!)
            ('veiculos', 'aeronaves', 'Aeronaves', {'vehicle_type': 'aeronave'}),
            ('veiculos', 'barcos', 'Barcos', {'vehicle_type': 'barco'}),
            ('veiculos', 'caminhoes', 'Caminhões', {'vehicle_type': 'caminhao'}),
            ('veiculos', 'carros', 'Carros', {'vehicle_type': 'carro'}),
            ('veiculos', 'motos', 'Motos', {'vehicle_type': 'moto'}),
            ('veiculos', 'onibus', 'Ônibus', {'vehicle_type': 'onibus'}),
            
            # OUTRAS CATEGORIAS (sem subcategoria e sem vehicle_type)
            ('imoveis', None, 'Imóveis', {}),
            ('bens-de-consumo', None, 'Bens de Consumo', {}),
            ('industrial', None, 'Industrial', {}),
            ('animais', None, 'Animais', {}),
            ('outros', None, 'Outros', {}),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_section': {},
            'duplicates': 0,
        }
    
    def scrape(self) -> List[dict]:
        """Scrape completo do MegaLeilões"""
        print("\n" + "="*60)
        print("🟢 MEGALEILÕES - DOMÍNIO COMPLETO")
        print("="*60)
        
        all_items = []
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
                
                # Varre cada seção principal
                for main_category, subcategory, display_name, extra_fields in self.main_sections:
                    print(f"\n📦 Seção: {display_name}")
                    
                    section_key = f"{main_category}/{subcategory}" if subcategory else main_category
                    section_items = self._scrape_section(
                        page, main_category, subcategory, display_name, extra_fields, global_ids
                    )
                    
                    all_items.extend(section_items)
                    self.stats['by_section'][section_key] = len(section_items)
                    
                    print(f"✅ {len(section_items)} itens coletados")
                    
                    # Pequeno delay entre seções
                    time.sleep(2)
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _get_cookies(self) -> List[dict]:
        """Captura cookies do MegaLeilões"""
        print("  🍪 Capturando cookies MegaLeilões...")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR'
                )
                
                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = {runtime: {}};
                """)
                
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
    
    def _scrape_section(self, page, main_category: str, subcategory: Optional[str],
                       display_name: str, extra_fields: dict, global_ids: set) -> List[dict]:
        """Scrape uma seção completa do MegaLeilões"""
        items = []
        page_num = 1
        consecutive_empty = 0
        max_empty = 3
        max_pages = 50
        
        while page_num <= max_pages and consecutive_empty < max_empty:
            # Monta URL da página
            if subcategory:
                # Veículos com subcategoria
                if page_num == 1:
                    url = f"{self.base_url}/{main_category}/{subcategory}"
                else:
                    url = f"{self.base_url}/{main_category}/{subcategory}?pagina={page_num}"
            else:
                # Outras categorias sem subcategoria
                if page_num == 1:
                    url = f"{self.base_url}/{main_category}"
                else:
                    url = f"{self.base_url}/{main_category}?pagina={page_num}"
            
            print(f"  Pág {page_num}", end='', flush=True)
            
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(random.uniform(3, 5))
                
                # Scroll para carregar conteúdo lazy
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(2)
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Seletor de cards do MegaLeilões
                cards = soup.select('div.card, .leilao-card, div[class*="card"]')
                
                if not cards:
                    print(f" ⚪ Sem cards")
                    consecutive_empty += 1
                    page_num += 1
                    continue
                
                novos = 0
                duplicados = 0
                filtrados = 0
                
                for card in cards:
                    item = self._extract_card(card, main_category, subcategory, display_name, extra_fields)
                    
                    if not item:
                        filtrados += 1
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
                    print(f" ✅ +{novos} | Total seção: {len(items)}")
                    consecutive_empty = 0
                else:
                    print(f" ⚪ 0 novos (dup: {duplicados}, filt: {filtrados})")
                    consecutive_empty += 1
                
                page_num += 1
                time.sleep(random.uniform(3, 6))
                
            except Exception as e:
                print(f" ❌ Erro: {str(e)[:80]}")
                consecutive_empty += 1
                page_num += 1
        
        return items
    
    def _extract_card(self, card, main_category: str, subcategory: Optional[str],
                     display_name: str, extra_fields: dict) -> Optional[dict]:
        """
        Extrai dados do card MegaLeilões.
        NÃO decide categoria final - apenas coleta dados brutos.
        Mantém vehicle_type quando disponível (veículos).
        """
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
                '/imoveis', '/veiculos', '/bens-de-consumo', '/industrial', '/animais', '/outros',
                '/carros', '/motos', '/caminhoes', '/onibus', '/casas', '/apartamentos',
                '/aeronaves', '/barcos'  # Adiciona as novas subcategorias
            ]
            
            if any(link_clean.endswith(ending) for ending in invalid_endings):
                return None
            
            # Extrai ID do item
            external_id = None
            parts = link.rstrip('/').split('/')
            for part in reversed(parts):
                if part and not part.startswith('?') and part not in ['imoveis', 'veiculos', 'bens-de-consumo', 'industrial', 'animais', 'outros', 'aeronaves', 'barcos']:
                    external_id = f"megaleiloes_{part.split('?')[0]}"
                    break
            
            if not external_id or external_id == 'megaleiloes_':
                return None
            
            # Texto do card (título + descrição)
            texto = card.get_text(separator=' ', strip=True)
            
            # Filtra cards de UI/paginação
            texto_lower = texto.lower()
            if 'exibindo' in texto_lower and 'itens' in texto_lower:
                return None
            
            if len(texto) < 10:
                return None
            
            # Título (primeiros 150 chars do texto)
            title = texto[:150].strip() if texto else f"Item {display_name}"
            
            # Preço
            value = None
            value_text = None
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
            
            # ✅ MONTA ITEM BASE - SEM DECIDIR CATEGORIA
            section_key = f"{main_category}/{subcategory}" if subcategory else main_category
            
            item = {
                'source': 'megaleiloes',
                'external_id': external_id,
                'title': title,
                'description': texto,
                'description_preview': texto[:200] if len(texto) > 200 else texto,
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
    print("🚀 MEGALEILÕES - SCRAPER COMPLETO DO DOMÍNIO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # ========================================
    # FASE 1: SCRAPE
    # ========================================
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = MegaLeiloesScraper()
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
    json_file = output_dir / f'megaleiloes_{timestamp}.json'
    
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
    print(f"🟢 MegaLeilões - Domínio Completo:")
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