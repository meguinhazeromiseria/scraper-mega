#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID - SCRAPER SIMPLIFICADO
Mapeamento direto: URL do site → Tabela do banco
Sem IA, sem keywords, apenas categorias oficiais do Superbid
"""

import sys
import json
import time
import random
import requests
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from normalizer import normalize_items


class SuperbidScraper:
    """Scraper com mapeamento direto de categorias do site"""
    
    def __init__(self):
        self.source = 'superbid'
        self.base_url = 'https://offer-query.superbid.net/seo/offers/'
        self.site_url = 'https://exchange.superbid.net'
        
        # MAPEAMENTO DIRETO: (url_slug, tabela_destino, nome_exibicao, campos_extras)
        self.sections = [
            # ========== ALIMENTOS E BEBIDAS ==========
            ('alimentos-e-bebidas', 'alimentos_bebidas', 'Alimentos e Bebidas', {}),
            
            # ========== ANIMAIS (1 tipo) ==========
            ('animais/bovinos', 'animais', 'Bovinos', {'animal_type': 'gado_bovino'}),
            
            # ========== ARTES E COLECIONISMO ==========
            ('artes-decoracao-colecionismo', 'artes_colecionismo', 'Artes e Colecionismo', {}),
            
            # ========== BENS DE CONSUMO (9 tipos) ==========
            ('bolsas-canetas-joias-e-relogios/acessorios', 'bens_consumo', 'Acessórios', {'consumption_goods_type': 'acessorios'}),
            ('bolsas-canetas-joias-e-relogios/bolsas', 'bens_consumo', 'Bolsas', {'consumption_goods_type': 'bolsas'}),
            ('bolsas-canetas-joias-e-relogios/canetas', 'bens_consumo', 'Canetas', {'consumption_goods_type': 'canetas'}),
            ('bolsas-canetas-joias-e-relogios/relogios', 'bens_consumo', 'Relógios', {'consumption_goods_type': 'relogios'}),
            ('oportunidades/beneficentes', 'bens_consumo', 'Beneficentes', {'consumption_goods_type': 'beneficentes'}),
            ('oportunidades/lazer', 'bens_consumo', 'Lazer', {'consumption_goods_type': 'lazer'}),
            ('oportunidades/esportes', 'bens_consumo', 'Esportes', {'consumption_goods_type': 'esportes'}),
            ('oportunidades/pet', 'bens_consumo', 'Pet', {'consumption_goods_type': 'pet'}),
            ('oportunidades/vestuarios', 'bens_consumo', 'Vestuários', {'consumption_goods_type': 'vestuarios'}),
            
            # ========== VEÍCULOS - CAMINHÕES/ÔNIBUS (4 tipos) ==========
            ('caminhoes-onibus/onibus', 'veiculos', 'Ônibus', {'vehicle_type': 'onibus'}),
            ('caminhoes-onibus/caminhoes', 'veiculos', 'Caminhões', {'vehicle_type': 'caminhao'}),
            ('caminhoes-onibus/vans', 'veiculos', 'Vans', {'vehicle_type': 'van'}),
            ('caminhoes-onibus/impl-rod-e-carrocerias', 'veiculos', 'Implementos Rodoviários', {'vehicle_type': 'implemento_rodoviario'}),
            
            # ========== VEÍCULOS - CARROS/MOTOS (3 tipos) ==========
            ('carros-motos/carros', 'veiculos', 'Carros', {'vehicle_type': 'carro'}),
            ('carros-motos/motos', 'veiculos', 'Motos', {'vehicle_type': 'moto'}),
            ('carros-motos/varias-ferramentas', 'veiculos', 'Veículos Diversos', {'vehicle_type': 'outro'}),
            
            # ========== VEÍCULOS - EMBARCAÇÕES/AERONAVES (4 tipos) ==========
            ('embarcacoes-aeronaves/embarcacoes-e-navios', 'veiculos', 'Embarcações e Navios', {'vehicle_type': 'barco'}),
            ('embarcacoes-aeronaves/jet-skis', 'veiculos', 'Jet Skis', {'vehicle_type': 'jetski'}),
            ('embarcacoes-aeronaves/lanchas-e-barcos', 'veiculos', 'Lanchas e Barcos', {'vehicle_type': 'barco'}),
            ('embarcacoes-aeronaves/avioes', 'veiculos', 'Aviões', {'vehicle_type': 'aeronave'}),
            
            # ========== PARTES E PEÇAS (4 tipos) ==========
            ('caminhoes-onibus/partes-e-pecas-caminhoes-e-onibus', 'partes_pecas', 'Peças Caminhões/Ônibus', {'parts_type': 'caminhoes_onibus'}),
            ('carros-motos/partes-e-pecas-carros-e-motos', 'partes_pecas', 'Peças Carros/Motos', {'parts_type': 'carros_motos'}),
            ('embarcacoes-aeronaves/pecas-e-acessorios', 'partes_pecas', 'Peças Embarcações/Aeronaves', {'parts_type': 'embarcacoes_aeronaves'}),
            ('partes-e-pecas', 'partes_pecas', 'Peças Variadas', {'parts_type': 'variados'}),
            
            # ========== NICHADOS (3 tipos) ==========
            ('cozinhas-e-restaurantes/restaurantes', 'nichados', 'Restaurantes', {'specialized_type': 'restaurante'}),
            ('cozinhas-e-restaurantes/cozinhas-industriais', 'nichados', 'Cozinhas Industriais', {'specialized_type': 'cozinha_industrial'}),
            ('oportunidades/negocios', 'nichados', 'Negócios', {'specialized_type': 'negocios'}),
            
            # ========== ELETRODOMÉSTICOS (4 tipos) ==========
            ('eletrodomesticos/refrigeradores', 'eletrodomesticos', 'Refrigeradores', {'appliance_type': 'refrigerador'}),
            ('eletrodomesticos/fornos-e-fogoes', 'eletrodomesticos', 'Fornos e Fogões', {'appliance_type': 'fogao_forno'}),
            ('eletrodomesticos/eletroportateis', 'eletrodomesticos', 'Eletroportáteis', {'appliance_type': 'eletroportatil'}),
            ('eletrodomesticos/limpeza', 'eletrodomesticos', 'Limpeza', {'appliance_type': 'limpeza'}),
            
            # ========== IMÓVEIS (6 tipos) ==========
            ('imoveis/imoveis-industriais', 'imoveis', 'Imóveis Industriais', {'property_type': 'galpao_industrial'}),
            ('imoveis/terrenos-e-lotes', 'imoveis', 'Terrenos e Lotes', {'property_type': 'terreno_lote'}),
            ('imoveis/imoveis-flutuantes', 'imoveis', 'Imóveis Flutuantes', {'property_type': 'flutuante'}),
            ('imoveis/imoveis-rurais', 'imoveis', 'Imóveis Rurais', {'property_type': 'rural'}),
            ('imoveis/imoveis-comerciais', 'imoveis', 'Imóveis Comerciais', {'property_type': 'comercial'}),
            ('imoveis/imoveis-residenciais', 'imoveis', 'Imóveis Residenciais', {'property_type': 'residencial'}),
            
            # ========== INDUSTRIAL EQUIPAMENTOS (3 categorias agregadas) ==========
            ('industrial-maquinas-equipamentos', 'industrial_equipamentos', 'Industrial e Máquinas', {}),
            ('movimentacao-transporte', 'industrial_equipamentos', 'Movimentação e Transporte', {}),
            ('oportunidades/teste', 'industrial_equipamentos', 'Equipamentos Teste', {}),
            
            # ========== MÁQUINAS PESADAS E AGRÍCOLAS ==========
            ('maquinas-pesadas-agricolas', 'maquinas_pesadas_agricolas', 'Máquinas Pesadas e Agrícolas', {}),
            
            # ========== MATERIAIS CONSTRUÇÃO (3 tipos) ==========
            ('materiais-para-construcao-civil/ferramentas', 'materiais_construcao', 'Ferramentas', {'construction_material_type': 'ferramentas'}),
            ('materiais-para-construcao-civil/materiais', 'materiais_construcao', 'Materiais', {'construction_material_type': 'materiais'}),
            ('materiais-para-construcao-civil/eletrica-e-iluminacao', 'materiais_construcao', 'Elétrica e Iluminação', {'construction_material_type': 'eletrica_iluminacao'}),
            
            # ========== MÓVEIS E DECORAÇÃO ==========
            ('moveis-e-decoracao', 'moveis_decoracao', 'Móveis e Decoração', {}),
            
            # ========== SUCATAS E RESÍDUOS ==========
            ('sucatas-materiais-residuos', 'sucatas_residuos', 'Sucatas e Resíduos', {}),
            
            # ========== TECNOLOGIA (3 tipos) ==========
            ('tecnologia/informatica', 'tecnologia', 'Informática', {'tech_type': 'informatica'}),
            ('tecnologia/telefonia', 'tecnologia', 'Telefonia', {'tech_type': 'telefonia'}),
            ('tecnologia/eletronicos', 'tecnologia', 'Eletrônicos', {'tech_type': 'eletronicos'}),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_table': defaultdict(int),
            'by_section': {},
            'duplicates': 0,
        }
        
        self.headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": "https://exchange.superbid.net",
            "referer": "https://exchange.superbid.net/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape(self) -> dict:
        """
        Scrape completo do Superbid
        Returns: dict com items agrupados por tabela
        """
        print("\n" + "="*60)
        print("🔵 SUPERBID - SCRAPER SIMPLIFICADO")
        print("="*60)
        
        items_by_table = defaultdict(list)
        global_ids = set()
        
        # Varre cada seção
        for url_slug, table, display_name, extra_fields in self.sections:
            print(f"\n📦 {display_name} → {table}")
            
            section_items = self._scrape_section(
                url_slug, table, display_name, extra_fields, global_ids
            )
            
            items_by_table[table].extend(section_items)
            self.stats['by_section'][url_slug] = len(section_items)
            self.stats['by_table'][table] += len(section_items)
            
            print(f"✅ {len(section_items)} itens → {table}")
            
            time.sleep(2)
        
        self.stats['total_scraped'] = sum(len(items) for items in items_by_table.values())
        return items_by_table
    
    def _scrape_section(self, url_slug: str, table: str, 
                       display_name: str, extra_fields: dict, 
                       global_ids: set) -> List[dict]:
        """Scrape uma seção específica via API"""
        items = []
        page_num = 1
        page_size = 100
        consecutive_errors = 0
        max_errors = 3
        max_pages = 100
        
        while page_num <= max_pages and consecutive_errors < max_errors:
            print(f"  Pág {page_num}", end='', flush=True)
            
            try:
                # Parâmetros da API Superbid
                params = {
                    "urlSeo": f"https://exchange.superbid.net/categorias/{url_slug}",
                    "locale": "pt_BR",
                    "orderBy": "score:desc",
                    "pageNumber": page_num,
                    "pageSize": page_size,
                    "portalId": "[2,15]",
                    "requestOrigin": "marketplace",
                    "searchType": "openedAll",
                    "timeZoneId": "America/Sao_Paulo",
                }
                
                response = self.session.get(
                    self.base_url,
                    params=params,
                    timeout=45
                )
                
                if response.status_code == 404:
                    print(f" ⚪ Fim")
                    break
                
                if response.status_code != 200:
                    print(f" ⚠️ Status {response.status_code}")
                    consecutive_errors += 1
                    time.sleep(5)
                    page_num += 1
                    continue
                
                data = response.json()
                offers = data.get("offers", [])
                
                if not offers:
                    print(f" ⚪ Vazia")
                    break
                
                novos = 0
                duplicados = 0
                
                for offer in offers:
                    item = self._extract_offer(offer, table, display_name, extra_fields)
                    
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
                    consecutive_errors = 0
                else:
                    print(f" ⚪ 0 novos (dup: {duplicados})")
                
                # Verifica se é última página
                if len(offers) < page_size:
                    break
                
                page_num += 1
                time.sleep(random.uniform(2, 5))
                
            except requests.exceptions.JSONDecodeError:
                print(f" ⚠️ Erro JSON")
                consecutive_errors += 1
                time.sleep(5)
                page_num += 1
            
            except Exception as e:
                print(f" ❌ Erro: {str(e)[:80]}")
                consecutive_errors += 1
                time.sleep(5)
                page_num += 1
        
        return items
    
    def _extract_offer(self, offer: dict, table: str, 
                      display_name: str, extra_fields: dict) -> Optional[dict]:
        """Extrai dados da oferta Superbid"""
        try:
            # Estrutura da resposta da API
            product = offer.get("product", {})
            auction = offer.get("auction", {})
            detail = offer.get("offerDetail", {})
            seller = offer.get("seller", {})
            store = offer.get("store", {})
            
            # ID externo
            offer_id = str(offer.get("id"))
            if not offer_id:
                return None
            
            external_id = f"superbid_{offer_id}"
            
            # Título
            title = (product.get("shortDesc") or "").strip()
            if not title or len(title) < 3:
                return None
            
            # Descrição
            description = offer.get("offerDescription", {}).get("offerDescription", "")
            
            # Valor
            value = detail.get("currentMinBid") or detail.get("initialBidValue")
            value_text = detail.get("currentMinBidFormatted") or detail.get("initialBidValueFormatted")
            
            # Localização
            city = None
            state = None
            seller_city = seller.get("city", "") or ""
            
            if '/' in seller_city:
                parts = seller_city.split('/')
                city = parts[0].strip()
                state = parts[1].strip() if len(parts) > 1 else None
            elif ' - ' in seller_city:
                parts = seller_city.split(' - ')
                city = parts[0].strip()
                state = parts[1].strip() if len(parts) > 1 else None
            
            # Link
            link = f"https://exchange.superbid.net/oferta/{offer_id}"
            
            # Data do leilão
            auction_date = None
            end_date_str = offer.get("endDate")
            if end_date_str:
                try:
                    # Formato ISO: "2025-01-15T15:00:00Z"
                    auction_date = end_date_str.replace('Z', ' ').replace('T', ' ').strip()
                except:
                    pass
            
            # Monta item base
            item = {
                'source': 'superbid',
                'external_id': external_id,
                'title': title,
                'description': description,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                'target_table': table,  # ✅ Tabela de destino já definida
                
                'auction_date': auction_date,
                'auction_type': auction.get("modalityDesc"),
                'auction_name': auction.get("desc"),
                'store_name': store.get("name"),
                'lot_number': offer.get("lotNumber"),
                
                'total_visits': offer.get("visits", 0),
                'total_bids': offer.get("totalBids", 0),
                'total_bidders': offer.get("totalBidders", 0),
                
                'metadata': {
                    'secao_site': display_name,
                    'leiloeiro': auction.get("auctioneer"),
                    'vendedor': seller.get("name"),
                }
            }
            
            # Adiciona campos extras (vehicle_type, property_type, etc.)
            if extra_fields:
                item.update(extra_fields)
            
            # Filtra itens de teste/demo
            store_name = str(store.get("name", "")).lower()
            if 'demo' in store_name or 'test' in store_name:
                return None
            
            # Valor muito baixo (suspeito)
            if value and value < 1:
                return None
            
            return item
            
        except Exception as e:
            return None


def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SUPERBID - SCRAPER SIMPLIFICADO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # ========================================
    # FASE 1: SCRAPE
    # ========================================
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SuperbidScraper()
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
    json_file = output_dir / f'superbid_{timestamp}.json'
    
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
    print(f"🔵 Superbid - Scraper Simplificado:")
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