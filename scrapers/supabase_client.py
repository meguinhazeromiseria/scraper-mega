#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - MEGALEILOES_ITEMS
✅ Cliente específico para tabela megaleiloes_items
✅ Suporta todos os campos da tabela incluindo informações de praça
✅ Validação de dados conforme schema
"""

import os
import time
import requests
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseMegaLeiloes:
    """Cliente Supabase para tabela megaleiloes_items"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        self.table = 'megaleiloes_items'
        
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Content-Profile': 'auctions',
            'Accept-Profile': 'auctions',
            'Prefer': 'resolution=merge-duplicates,return=minimal'
        }
        
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def upsert(self, items: List[Dict]) -> Dict:
        """Upsert de itens na tabela megaleiloes_items"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Prepara itens
        prepared = []
        for item in items:
            try:
                db_item = self._prepare_item(item)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                print(f"  ⚠️ Erro ao preparar item: {e}")
        
        if not prepared:
            print("  ⚠️ Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # Insere em batches
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(prepared) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{self.table}"
        
        for i in range(0, len(prepared), batch_size):
            batch = prepared[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens")
                elif r.status_code == 409:
                    stats['updated'] += len(batch)
                    print(f"  🔄 Batch {batch_num}/{total_batches}: {len(batch)} atualizados")
                else:
                    error_msg = r.text[:200] if r.text else 'Sem detalhes'
                    print(f"  ❌ Batch {batch_num}: HTTP {r.status_code} - {error_msg}")
                    stats['errors'] += len(batch)
            
            except Exception as e:
                print(f"  ❌ Batch {batch_num}: {e}")
                stats['errors'] += len(batch)
            
            if batch_num < total_batches:
                time.sleep(0.5)
        
        return stats
    
    def _prepare_item(self, item: Dict) -> Optional[Dict]:
        """Prepara item para inserção validando campos"""
        external_id = item.get('external_id')
        if not external_id:
            return None
        
        # Processa auction_date
        auction_date = item.get('auction_date')
        if auction_date and isinstance(auction_date, str):
            try:
                auction_date = auction_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(auction_date)
                auction_date = dt.isoformat()
            except:
                auction_date = None
        
        # Processa first_round_date
        first_round_date = item.get('first_round_date')
        if first_round_date and isinstance(first_round_date, str):
            try:
                first_round_date = first_round_date.replace('Z', '+00:00')
                dt = datetime.fromisoformat(first_round_date)
                first_round_date = dt.isoformat()
            except:
                first_round_date = None
        
        # Valida state
        state = item.get('state')
        if state:
            state = str(state).strip().upper()
            if len(state) != 2:
                state = None
        
        # Valida value
        value = item.get('value')
        if value is not None:
            try:
                value = float(value)
                if value < 0:
                    value = None
            except:
                value = None
        
        # Valida first_round_value
        first_round_value = item.get('first_round_value')
        if first_round_value is not None:
            try:
                first_round_value = float(first_round_value)
                if first_round_value < 0:
                    first_round_value = None
            except:
                first_round_value = None
        
        # Valida discount_percentage
        discount_percentage = item.get('discount_percentage')
        if discount_percentage is not None:
            try:
                discount_percentage = float(discount_percentage)
            except:
                discount_percentage = None
        
        # Valida auction_round (1 ou 2)
        auction_round = item.get('auction_round')
        if auction_round is not None:
            try:
                auction_round = int(auction_round)
                if auction_round not in (1, 2):
                    auction_round = None
            except:
                auction_round = None
        
        # Processa has_bid
        has_bid = item.get('has_bid')
        if has_bid is None:
            has_bid = False
        elif not isinstance(has_bid, bool):
            has_bid = str(has_bid).lower() in ('true', '1', 'yes', 'sim')
        
        # Processa metadata
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # Monta item com todos os campos da tabela
        data = {
            'external_id': str(external_id),
            'category': str(item.get('category')) if item.get('category') else None,
            'title': str(item.get('title', 'Sem Título'))[:1000],  # TEXT sem limite, mas vamos limitar
            'description': str(item.get('description')) if item.get('description') else None,
            'city': str(item.get('city')) if item.get('city') else None,
            'state': state,
            'value': value,
            'value_text': str(item.get('value_text')) if item.get('value_text') else None,
            'auction_round': auction_round,
            'auction_date': auction_date,
            'first_round_value': first_round_value,
            'first_round_date': first_round_date,
            'discount_percentage': discount_percentage,
            'link': str(item.get('link')) if item.get('link') else None,
            'source': str(item.get('source', 'megaleiloes')),
            'metadata': metadata,
            'is_active': True,
            'has_bid': has_bid,
            'auction_type': str(item.get('auction_type')) if item.get('auction_type') else None,
            'last_scraped_at': datetime.now().isoformat(),
        }
        
        return data
    
    def test(self) -> bool:
        """Testa conexão com Supabase"""
        try:
            url = f"{self.url}/rest/v1/"
            r = self.session.get(url, timeout=10)
            
            if r.status_code == 200:
                print("✅ Conexão com Supabase OK")
                return True
            else:
                print(f"❌ Erro HTTP {r.status_code}")
                return False
        except Exception as e:
            print(f"❌ Erro: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas da tabela"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0').split('/')[-1])
                return {'total': total, 'table': self.table}
        except:
            pass
        
        return {'total': 0, 'table': self.table}
    
    def get_by_category(self, category: str, limit: int = 100) -> List[Dict]:
        """Busca itens por categoria"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'category': f'eq.{category}',
                'is_active': 'eq.true',
                'order': 'created_at.desc',
                'limit': limit
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
        except:
            pass
        
        return []
    
    def get_by_round(self, auction_round: int, limit: int = 100) -> List[Dict]:
        """Busca itens por praça"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'auction_round': f'eq.{auction_round}',
                'is_active': 'eq.true',
                'order': 'value.asc',
                'limit': limit
            }
            
            r = self.session.get(url, params=params, timeout=30)
            
            if r.status_code == 200:
                return r.json()
        except:
            pass
        
        return []
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


if __name__ == "__main__":
    # Teste do cliente
    print("🧪 Testando SupabaseMegaLeiloes\n")
    
    client = SupabaseMegaLeiloes()
    
    if client.test():
        stats = client.get_stats()
        print(f"\n📊 Estatísticas:")
        print(f"  Total de itens: {stats['total']}")
        
        # Exemplo de busca por categoria
        carros = client.get_by_category('Carros', limit=5)
        print(f"\n🚗 Primeiros carros: {len(carros)} itens")
        
        # Exemplo de busca por praça
        segunda_praca = client.get_by_round(2, limit=5)
        print(f"💰 Segunda praça: {len(segunda_praca)} itens")