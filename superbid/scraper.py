#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERBID - SCRAPER COMPLETO DO DOMÍNIO
Varre TODO o site Superbid via API REST
Scrape → Normalize → Classify → Insert
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

# Adiciona pasta pai ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from supabase_client import SupabaseClient
from groq_classifier import GroqTableClassifier
from normalizer import normalize_items


class SuperbidScraper:
    """Scraper completo do domínio Superbid via API REST"""
    
    def __init__(self):
        self.source = 'superbid'
        self.base_url = 'https://offer-query.superbid.net/seo/offers/'
        self.site_url = 'https://exchange.superbid.net'
        self.session = requests.Session()
        
        # Todas as seções do Superbid
        # Para veículos: usa subcategorias específicas com vehicle_type
        # Para outras: usa categoria principal
        self.main_sections = [
            # VEÍCULOS (subcategorias específicas para manter vehicle_type)
            ('carros-motos/carros', 'Carros', {'vehicle_type': 'carro'}),
            ('carros-motos/motos', 'Motos', {'vehicle_type': 'moto'}),
            ('caminhoes-onibus/caminhoes', 'Caminhões', {'vehicle_type': 'caminhao'}),
            ('caminhoes-onibus/onibus', 'Ônibus', {'vehicle_type': 'onibus'}),
            ('caminhoes-onibus/vans', 'Vans', {'vehicle_type': 'van'}),
            
            # OUTRAS CATEGORIAS (sem vehicle_type)
            ('embarcacoes-aeronaves', 'Embarcações e Aeronaves', {}),
            ('imoveis', 'Imóveis', {}),
            ('tecnologia', 'Tecnologia', {}),
            ('eletrodomesticos', 'Eletrodomésticos', {}),
            ('industrial-maquinas-equipamentos', 'Industrial, Máquinas e Equipamentos', {}),
            ('maquinas-pesadas-agricolas', 'Máquinas Pesadas e Agrícolas', {}),
            ('materiais-para-construcao-civil', 'Materiais para Construção Civil', {}),
            ('moveis-e-decoracao', 'Móveis e Decoração', {}),
            ('cozinhas-e-restaurantes', 'Cozinhas e Restaurantes', {}),
            ('movimentacao-transporte', 'Movimentação e Transporte', {}),
            ('partes-e-pecas', 'Partes e Peças', {}),
            ('sucatas-materiais-residuos', 'Sucatas, Materiais e Resíduos', {}),
            ('alimentos-e-bebidas', 'Alimentos e Bebidas', {}),
            ('animais', 'Animais', {}),
            ('artes-decoracao-colecionismo', 'Artes, Decoração e Colecionismo', {}),
            ('bolsas-canetas-joias-e-relogios', 'Bolsas, Canetas, Joias e Relógios', {}),
            ('oportunidades', 'Oportunidades', {}),
        ]
        
        self.stats = {
            'total_scraped': 0,
            'by_section': {},
            'duplicates': 0,
        }
        
        # Headers padrão para API
        self.headers = {
            "accept": "*/*",
            "accept-language": "pt-BR,pt;q=0.9",
            "origin": "https://exchange.superbid.net",
            "referer": "https://exchange.superbid.net/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
    
    def scrape(self) -> List[dict]:
        """Scrape completo do Superbid"""
        print("\n" + "="*60)
        print("🔵 SUPERBID - DOMÍNIO COMPLETO")
        print("="*60)
        
        all_items = []
        global_ids = set()
        
        # Varre cada seção principal
        for section_slug, section_name, extra_fields in self.main_sections:
            print(f"\n📦 Seção: {section_name}")
            section_items = self._scrape_section(section_slug, section_name, extra_fields, global_ids)
            
            all_items.extend(section_items)
            self.stats['by_section'][section_slug] = len(section_items)
            
            print(f"✅ {len(section_items)} itens coletados")
            
            # Delay entre seções
            time.sleep(2)
        
        self.stats['total_scraped'] = len(all_items)
        return all_items
    
    def _scrape_section(self, section_slug: str, section_name: str, extra_fields: dict, global_ids: set) -> List[dict]:
        """Scrape uma seção completa do Superbid via API"""
        items = []
        page_num = 1
        page_size = 100
        consecutive_errors = 0
        max_errors = 3
        max_pages = 100
        
        while page_num <= max_pages and consecutive_errors < max_errors:
            print(f"  Pág {page_num}", end='', flush=True)
            
            try:
                # Parâmetros da API (baseado no código que funcionava)
                params = {
                    "urlSeo": f"https://exchange.superbid.net/categorias/{section_slug}",
                    "locale": "pt_BR",
                    "orderBy": "offerDetail.percentDiffReservedPriceOverFipePrice:asc",
                    "pageNumber": page_num,
                    "pageSize": page_size,
                    "portalId": "[2,15]",
                    "preOrderBy": "orderByFirstOpenedOffersAndSecondHasPhoto",
                    "requestOrigin": "marketplace",
                    "searchType": "openedAll",
                    "timeZoneId": "America/Sao_Paulo",
                }
                
                # Request na API
                response = self.session.get(
                    self.base_url,
                    params=params,
                    headers=self.headers,
                    timeout=45
                )
                
                # Tratamento de erros
                if response.status_code == 404:
                    print(f" ⚪ Fim (404)")
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
                    item = self._extract_offer(offer, section_slug, section_name, extra_fields)
                    
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
                    print(f" ✅ +{novos} | Total seção: {len(items)}")
                    consecutive_errors = 0
                else:
                    print(f" ⚪ 0 novos (dup: {duplicados})")
                
                # Verifica se é última página
                if len(offers) < page_size:
                    print("  ✅ Última página")
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
    
    def _extract_offer(self, offer: dict, section_slug: str, section_name: str, extra_fields: dict) -> Optional[dict]:
        """
        Extrai dados da oferta Superbid.
        NÃO decide categoria final - apenas coleta dados brutos.
        Mantém vehicle_type quando disponível (veículos).
        """
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
            
            # Descrição completa
            full_desc = offer.get("offerDescription", {}).get("offerDescription", "")
            description_preview = full_desc[:200] if full_desc else title[:200]
            
            # Valor
            value = detail.get("currentMinBid") or detail.get("initialBidValue")
            value_text = detail.get("currentMinBidFormatted") or detail.get("initialBidValueFormatted")
            
            # Localização (formato: "Cidade/UF" ou "Cidade - UF")
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
            
            # Valida UF
            if state and (len(state) != 2 or not state.isupper()):
                state = None
            
            # Link
            link = f"https://exchange.superbid.net/oferta/{offer_id}"
            
            # Data do leilão
            auction_date = None
            end_date_str = offer.get("endDate")
            if end_date_str:
                try:
                    auction_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                except:
                    pass
            
            # ✅ MONTA ITEM BASE - SEM DECIDIR CATEGORIA
            item = {
                'source': 'superbid',
                'external_id': external_id,
                'title': title,
                'description': full_desc,
                'description_preview': description_preview,
                'value': value,
                'value_text': value_text,
                'city': city,
                'state': state,
                'link': link,
                
                # Categoria ORIGINAL do site (só metadata, não decisão)
                'raw_category': section_slug,
                
                'metadata': {
                    'secao_site': section_name,
                    'secao_slug': section_slug,
                    'leilao_tipo': auction.get("modalityDesc"),
                    'leilao_nome': auction.get("desc"),
                    'leiloeiro': auction.get("auctioneer"),
                    'loja_nome': store.get("name"),
                    'vendedor': seller.get("name"),
                    'lote_numero': offer.get("lotNumber"),
                    'total_visitas': offer.get("visits", 0),
                    'total_lances': offer.get("totalBids", 0),
                    'total_participantes': offer.get("totalBidders", 0),
                    'data_leilao': auction_date.isoformat() if auction_date else None,
                }
            }
            
            # ✅ ADICIONA VEHICLE_TYPE APENAS PARA VEÍCULOS
            # Isso ajuda os handlers de busca, mas NÃO define a tabela final
            if 'vehicle_type' in extra_fields:
                item['vehicle_type'] = extra_fields['vehicle_type']
            
            # Filtra itens de teste/demo
            store_name = str(store.get("name", "")).lower()
            if not store.get("name") or 'demo' in store_name or 'test' in store_name:
                return None
            
            # Valor muito baixo (suspeito)
            if value and value < 1:
                return None
            
            return item
            
        except Exception as e:
            # Silencioso - não loga cada erro de parsing
            return None


def main():
    """Execução principal"""
    print("\n" + "="*70)
    print("🚀 SUPERBID - SCRAPER COMPLETO DO DOMÍNIO")
    print("="*70)
    print(f"📅 Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    start_time = time.time()
    
    # ========================================
    # FASE 1: SCRAPE
    # ========================================
    print("\n🔥 FASE 1: COLETANDO DADOS")
    scraper = SuperbidScraper()
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
    json_file = output_dir / f'superbid_{timestamp}.json'
    
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
    print(f"🔵 Superbid - Domínio Completo:")
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