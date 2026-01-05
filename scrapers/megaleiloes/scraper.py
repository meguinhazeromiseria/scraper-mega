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

# Adiciona pasta pai ao path
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
        self.main_sections = [
            # VEÍCULOS (6 subcategorias)
            ('veiculos', 'aeronaves', 'Aeronaves', {'vehicle_type': 'aeronave'}),
            ('veiculos', 'barcos', 'Barcos', {'vehicle_type': 'barco'}),
            ('veiculos', 'caminhoes', 'Caminhões', {'vehicle_type': 'caminhao'}),
            ('veiculos', 'carros', 'Carros', {'vehicle_type': 'carro'}),
            ('veiculos', 'motos', 'Motos', {'vehicle_type': 'moto'}),
            ('veiculos', 'onibus', 'Ônibus', {'vehicle_type': 'onibus'}),
            
            # OUTRAS CATEGORIAS
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
                browser = p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='pt-BR',
                    timezone_id='America/Sao_Paulo'
                )
                
                # Anti-detecção
                context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = {runtime: {}};
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                """)
                
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
                    
                    # Delay entre seções
                    time.sleep(random.uniform(2, 4))
                
                browser.close()
        
        except Exception as e:
            print(f"❌ Erro geral: {e}")
            import traceback
            traceback.print_exc()
        
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
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
                if page_num == 1:
                    url = f"{self.base_url}/{main_category}/{subcategory}"
                else:
                    url = f"{self.base_url}/{main_category}/{subcategory}?pagina={page_num}"
            else:
                if page_num == 1:
                    url = f"{self.base_url}/{main_category}"
                else:
                    url = f"{self.base_url}/{main_category}?pagina={page_num}"
            
            print(f"  Pág {page_num}", end='', flush=True)
            
            try:
                response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # Verifica se a página carregou
                if not response or response.status >= 400:
                    print(f" ❌ HTTP {response.status if response else 'timeout'}")
                    consecutive_empty += 1
                    page_num += 1
                    continue
                
                # Aguarda conteúdo carregar
                time.sleep(random.uniform(2, 4))
                
                # Scroll para carregar lazy loading
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                time.sleep(1)
                page.evaluate("window.scrollTo(0, 0)")
                time.sleep(1)
                
                html = page.content()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Múltiplos seletores para capturar cards
                cards = []
                
                # Tenta seletores comuns de cards de leilão
                selectors = [
                    'div.card',
                    'div.leilao-card',
                    'div[class*="card-"]',
                    'div[class*="item-"]',
                    'article',
                    'div.produto',
                    'div.lot',
                    'div.lote',
                    'a.card',
                    'a[href*="leilao"]',
                ]
                
                for selector in selectors:
                    found = soup.select(selector)
                    if found:
                        cards = found
                        break
                
                # Se não achou nada, tenta capturar qualquer link com href
                if not cards:
                    cards = soup.find_all('a', href=True)
                
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
                
                # Delay entre páginas
                time.sleep(random.uniform(2, 5))
                
            except Exception as e:
                print(f" ❌ Erro: {str(e)[:60]}")
                consecutive_empty += 1
                page_num += 1
                time.sleep(3)
        
        return items
    
    def _extract_card(self, card, main_category: str, subcategory: Optional[str],
                     display_name: str, extra_fields: dict) -> Optional[dict]:
        """
        Extrai dados do card MegaLeilões.
        Coleta dados brutos sem decidir categoria final.
        """
        try:
            # Link - tenta em diferentes lugares
            link = None
            
            # Se o card em si é um link
            if card.name == 'a' and card.get('href'):
                link = card.get('href')
            else:
                # Procura link dentro do card
                link_elem = card.select_one('a[href]')
                if link_elem:
                    link = link_elem.get('href', '')
            
            if not link or 'javascript' in link.lower() or link == '#':
                return None
            
            # Normaliza link
            link = link.strip()
            if not link.startswith('http'):
                if link.startswith('/'):
                    link = f"{self.base_url}{link}"
                else:
                    link = f"{self.base_url}/{link}"
            
            # Remove query strings e âncoras para validação
            link_clean = link.split('?')[0].split('#')[0].rstrip('/')
            
            # Filtra links de navegação/categoria (não são itens)
            invalid_patterns = [
                '/imoveis', '/veiculos', '/bens-de-consumo', '/industrial', 
                '/animais', '/outros', '/carros', '/motos', '/caminhoes', 
                '/onibus', '/aeronaves', '/barcos', '/casas', '/apartamentos',
                '/pagina', '/categoria', '/busca', '/pesquisa'
            ]
            
            # Verifica se é um link de categoria
            if any(link_clean.endswith(pattern) for pattern in invalid_patterns):
                return None
            
            # Link deve ter pelo menos um segmento depois da categoria
            parts = link_clean.split('/')
            if len(parts) < 5:  # protocolo + dominio + categoria + subcategoria? + item
                return None
            
            # Extrai ID único do item
            external_id = None
            for part in reversed(parts):
                if part and len(part) > 2:
                    # Evita usar nomes de categorias como ID
                    if part not in ['imoveis', 'veiculos', 'bens-de-consumo', 'industrial', 
                                   'animais', 'outros', 'carros', 'motos', 'caminhoes',
                                   'onibus', 'aeronaves', 'barcos']:
                        external_id = f"megaleiloes_{part}"
                        break
            
            if not external_id or external_id == 'megaleiloes_':
                return None
            
            # Extrai texto completo do card
            texto = card.get_text(separator=' ', strip=True)
            
            # Filtra elementos de UI/navegação
            texto_lower = texto.lower()
            ui_keywords = [
                'exibindo', 'resultados', 'página', 'anterior', 'próxima',
                'filtrar', 'ordenar', 'buscar', 'pesquisar', 'menu'
            ]
            
            if any(keyword in texto_lower for keyword in ui_keywords) and len(texto) < 50:
                return None
            
            if len(texto) < 15:  # Texto muito curto
                return None
            
            # Título (usa primeiros 200 chars ou extrai de h1-h6)
            title = None
            for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                heading = card.select_one(tag)
                if heading:
                    title = heading.get_text(strip=True)
                    break
            
            if not title:
                # Tenta pegar texto do link principal
                if card.name == 'a':
                    title = card.get_text(strip=True)
                else:
                    link_elem = card.select_one('a')
                    if link_elem:
                        title = link_elem.get_text(strip=True)
            
            if not title or len(title) < 5:
                title = texto[:200].strip()
            
            # Limpa título
            title = title[:255].strip()
            
            # Extrai preço
            value = None
            value_text = None
            
            # Padrões de preço
            price_patterns = [
                r'R\$\s*([\d.]+,\d{2})',
                r'R\$\s*([\d.]+)',
                r'lance\s+atual[:\s]+([\d.,]+)',
                r'valor[:\s]+R\$\s*([\d.]+,\d{2})',
            ]
            
            for pattern in price_patterns:
                match = re.search(pattern, texto, re.IGNORECASE)
                if match:
                    value_text = f"R$ {match.group(1)}"
                    try:
                        value_str = match.group(1).replace('.', '').replace(',', '.')
                        value = float(value_str)
                    except:
                        pass
                    break
            
            # Extrai estado (UF)
            state = None
            state_match = re.search(r'\b([A-Z]{2})\b', texto)
            if state_match:
                uf = state_match.group(1)
                valid_states = [
                    'AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG',
                    'PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO'
                ]
                if uf in valid_states:
                    state = uf
            
            # Extrai cidade
            city = None
            city_patterns = [
                r'([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+)*)\s*[-–/,]\s*[A-Z]{2}',
                r'cidade[:\s]+([A-ZÀ-Ú][a-zà-ú\s]+)',
            ]
            
            for pattern in city_patterns:
                match = re.search(pattern, texto)
                if match:
                    city = match.group(1).strip()
                    break
            
            # Data do leilão
            auction_date = None
            date_patterns = [
                r'(\d{2})/(\d{2})/(\d{4})',
                r'(\d{4})-(\d{2})-(\d{2})',
            ]
            
            for pattern in date_patterns:
                match = re.search(pattern, texto)
                if match:
                    try:
                        if '/' in pattern:
                            day, month, year = match.groups()
                        else:
                            year, month, day = match.groups()
                        auction_date = f"{year}-{month}-{day}T00:00:00"
                    except:
                        pass
                    break
            
            # Monta item base
            section_key = f"{main_category}/{subcategory}" if subcategory else main_category
            
            item = {
                'source': 'megaleiloes',
                'external_id': external_id,
                'title': title,
                'description': texto[:3000] if len(texto) > 3000 else texto,
                'description_preview': texto[:200] if len(texto) > 200 else texto,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'auction_date': auction_date,
                'link': link,
                'raw_category': section_key,
                
                'metadata': {
                    'secao_site': display_name,
                    'categoria_principal': main_category,
                    'subcategoria': subcategory,
                }
            }
            
            # Adiciona vehicle_type para veículos (ajuda na busca, mas não define tabela)
            if 'vehicle_type' in extra_fields:
                item['vehicle_type'] = extra_fields['vehicle_type']
                item['metadata']['vehicle_type'] = extra_fields['vehicle_type']
            
            return item
            
        except Exception as e:
            # Silencioso para não poluir output
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
        import traceback
        traceback.print_exc()
        print("Usando dados brutos...")
        normalized_items = items
    
    # Salva JSON normalizado (para debug)
    output_dir = Path(__file__).parent / 'data' / 'normalized'
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = output_dir / f'megaleiloes_{timestamp}.json'
    
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(normalized_items, f, ensure_ascii=False, indent=2)
        print(f"💾 JSON salvo: {json_file}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar JSON: {e}")
    
    # ========================================
    # FASE 3: CLASSIFICAÇÃO GROQ
    # ========================================
    print("\n🤖 FASE 3: CLASSIFICANDO COM GROQ AI")
    try:
        classifier = GroqTableClassifier()
        items_by_table = defaultdict(list)
        
        total = len(normalized_items)
        for i, item in enumerate(normalized_items, 1):
            if i % 10 == 0 or i == total:
                print(f"  ⏳ {i}/{total}", end='\r', flush=True)
            
            table = classifier.classify(item)
            if table:
                items_by_table[table].append(item)
            else:
                items_by_table['oportunidades'].append(item)
            
            # Rate limit Groq
            if i < total:
                time.sleep(0.3)
        
        print(f"\n✅ Classificação concluída!")
        print(f"\n📊 Distribuição por tabela:")
        for table, table_items in sorted(items_by_table.items(), key=lambda x: len(x[1]), reverse=True):
            percentage = (len(table_items) / total * 100) if total > 0 else 0
            print(f"  • {table}: {len(table_items)} itens ({percentage:.1f}%)")
        
        # Stats do classifier
        print()
        classifier.print_stats()
    
    except Exception as e:
        print(f"⚠️ Erro na classificação: {e}")
        import traceback
        traceback.print_exc()
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
            total_errors = 0
            
            for table, table_items in sorted(items_by_table.items()):
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
                total_errors += stats['errors']
            
            print(f"\n  📈 TOTAL GERAL:")
            print(f"    ✅ Inseridos: {total_inserted}")
            print(f"    🔄 Atualizados: {total_updated}")
            if total_errors > 0:
                print(f"    ⚠️ Erros: {total_errors}")
    
    except Exception as e:
        print(f"⚠️ Erro no Supabase: {e}")
        import traceback
        traceback.print_exc()
    
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
    for section, count in sorted(scraper.stats['by_section'].items(), key=lambda x: x[1], reverse=True):
        print(f"    • {section}: {count} itens")
    print(f"\n  • Total coletado: {scraper.stats['total_scraped']}")
    print(f"  • Duplicatas: {scraper.stats['duplicates']}")
    print(f"\n⏱️ Duração: {minutes}min {seconds}s")
    print(f"✅ Concluído: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)


if __name__ == "__main__":
    main()