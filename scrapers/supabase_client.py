#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""SUPABASE CLIENT - Genérico para todas as tabelas"""

import os
import time
import requests
from datetime import datetime


class SupabaseClient:
    """Cliente para Supabase - Schema auctions"""
    
    def __init__(self):
        self.url = os.getenv('SUPABASE_URL')
        self.key = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
        
        if not self.url or not self.key:
            raise ValueError("❌ Configure SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY")
        
        self.url = self.url.rstrip('/')
        
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
    
    def upsert(self, tabela: str, items: list) -> dict:
        """Upsert com suporte a campos específicos por tabela"""
        if not items:
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        prepared = []
        for item in items:
            try:
                db_item = self._prepare(item, tabela)
                if db_item:
                    prepared.append(db_item)
            except Exception as e:
                print(f"  ⚠️ Erro ao preparar item: {e}")
        
        if not prepared:
            print("  ⚠️ Nenhum item válido para inserir")
            return {'inserted': 0, 'updated': 0, 'errors': 0}
        
        # 🔧 Normaliza chaves do batch (remove campos inválidos por tabela)
        prepared = self._normalize_batch_keys(prepared, tabela)
        
        stats = {'inserted': 0, 'updated': 0, 'errors': 0}
        batch_size = 500
        total_batches = (len(prepared) + batch_size - 1) // batch_size
        
        url = f"{self.url}/rest/v1/{tabela}"
        
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
    
    def _normalize_batch_keys(self, items: list, tabela: str = '') -> list:
        """Garante que todos os itens tenham apenas campos válidos para a tabela"""
        if not items:
            return items
        
        # Campos padrão presentes em TODAS as tabelas
        standard_fields = {
            'source', 'external_id', 'title', 'normalized_title', 'description_preview',
            'description', 'value', 'value_text', 'city', 'state', 'address',
            'auction_date', 'days_remaining', 'auction_type', 'auction_name',
            'store_name', 'lot_number', 'total_visits', 'total_bids', 'total_bidders',
            'link', 'metadata', 'duplicate_group', 'is_primary_duplicate',
            'is_active', 'created_at', 'updated_at', 'last_scraped_at',
            'market_price', 'market_price_source', 'market_price_updated_at',
            'market_price_confidence', 'market_price_metadata'
        }
        
        # Campos específicos por tabela
        table_specific_fields = {
            'veiculos': {'vehicle_type'},
            'tecnologia': {'multiplecategory'},
            # Outras tabelas só têm campos padrão
        }
        
        # Campos permitidos para esta tabela
        allowed_fields = standard_fields.copy()
        if tabela in table_specific_fields:
            allowed_fields.update(table_specific_fields[tabela])
        
        # Coleta todas as chaves válidas do batch
        all_keys = set()
        for item in items:
            for key in item.keys():
                if key in allowed_fields:
                    all_keys.add(key)
        
        # Normaliza cada item para ter todas as chaves válidas
        normalized = []
        for item in items:
            normalized_item = {}
            for key in all_keys:
                normalized_item[key] = item.get(key, None)
            normalized.append(normalized_item)
        
        return normalized
    
    def _prepare(self, item: dict, tabela: str = '') -> dict:
        """Prepara item para inserção no banco"""
        source = item.get('source')
        external_id = item.get('external_id')
        title = item.get('title') or 'Sem título'
        
        if not source or not external_id:
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
        
        # Valida state
        state = item.get('state')
        if state:
            state = str(state).strip().upper()
            if len(state) != 2:
                state = None
        
        # Processa value
        value = item.get('value')
        if value is not None:
            try:
                value = float(value)
                if value < 0:
                    value = None
            except:
                value = None
        
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # ✅ Campos padrão (presentes em TODAS as tabelas)
        data = {
            'source': str(source),
            'external_id': str(external_id),
            'title': str(title)[:255],
            'normalized_title': str(item.get('normalized_title') or title)[:255],
            'description_preview': str(item.get('description_preview', ''))[:255] if item.get('description_preview') else None,
            'description': str(item.get('description')) if item.get('description') else None,
            'value': value,
            'value_text': str(item.get('value_text')) if item.get('value_text') else None,
            'city': str(item.get('city')) if item.get('city') else None,
            'state': state,
            'address': str(item.get('address')) if item.get('address') else None,
            'auction_date': auction_date,
            'days_remaining': int(item.get('days_remaining', 0)) if item.get('days_remaining') is not None else None,
            'auction_type': str(item.get('auction_type', 'Leilão'))[:100],
            'auction_name': str(item.get('auction_name')) if item.get('auction_name') else None,
            'store_name': str(item.get('store_name')) if item.get('store_name') else None,
            'lot_number': str(item.get('lot_number')) if item.get('lot_number') else None,
            'total_visits': int(item.get('total_visits', 0)),
            'total_bids': int(item.get('total_bids', 0)),
            'total_bidders': int(item.get('total_bidders', 0)),
            'link': str(item.get('link')) if item.get('link') else None,
            'metadata': metadata,
            'is_active': True,
            'last_scraped_at': datetime.now().isoformat(),
        }
        
        # ✅ Campos específicos por tabela
        if tabela == 'veiculos':
            # Tenta pegar do root primeiro, depois do metadata
            vehicle_type = item.get('vehicle_type')
            if not vehicle_type and isinstance(metadata, dict):
                vehicle_type = metadata.get('vehicle_type')
            
            if vehicle_type:
                data['vehicle_type'] = str(vehicle_type)[:255]
        
        if tabela == 'tecnologia':
            multiplecategory = item.get('multiplecategory')
            if not multiplecategory and isinstance(metadata, dict):
                multiplecategory = metadata.get('multiplecategory')
            
            if multiplecategory and isinstance(multiplecategory, list):
                data['multiplecategory'] = multiplecategory
        
        return data
    
    def test(self) -> bool:
        """Testa conexão"""
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
    
    def get_stats(self, tabela: str) -> dict:
        """Retorna estatísticas"""
        try:
            url = f"{self.url}/rest/v1/{tabela}"
            r = self.session.get(
                url,
                params={'select': 'count'},
                headers={**self.headers, 'Prefer': 'count=exact'},
                timeout=30
            )
            
            if r.status_code == 200:
                total = int(r.headers.get('Content-Range', '0').split('/')[-1])
                return {'total': total, 'table': tabela}
        except:
            pass
        
        return {'total': 0, 'table': tabela}
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()