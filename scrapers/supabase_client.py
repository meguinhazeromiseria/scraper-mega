#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPABASE CLIENT - MEGALEILOES_ITEMS + HEARTBEAT
✅ Cliente específico para tabela megaleiloes_items
✅ Sistema de heartbeat integrado (infra_actions)
✅ Validação de dados conforme schema
✅ UPSERT correto com on_conflict=external_id
"""

import os
import time
import requests
import traceback
from datetime import datetime
from typing import List, Dict, Optional


class SupabaseMegaLeiloes:
    """Cliente Supabase para tabela megaleiloes_items com heartbeat integrado"""
    
    def __init__(self, service_name: str = 'megaleiloes_scraper'):
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
        
        # ============================================
        # HEARTBEAT - Configuração
        # ============================================
        self.service_name = service_name
        self.heartbeat_enabled = True
        self.heartbeat_start_time = time.time()
        self.heartbeat_metrics = {
            'items_processed': 0,
            'pages_scraped': 0,
            'errors': 0,
            'warnings': 0,
        }
    
    # ============================================
    # MÉTODOS HEARTBEAT
    # ============================================
    
    def _send_heartbeat(self, status: str, logs: Optional[Dict] = None, 
                        error_message: Optional[str] = None, 
                        metadata: Optional[Dict] = None) -> bool:
        """Envia heartbeat para infra_actions"""
        if not self.heartbeat_enabled:
            return False
        
        try:
            elapsed = time.time() - self.heartbeat_start_time
            
            full_logs = {
                'timestamp': datetime.now().isoformat(),
                'elapsed_seconds': round(elapsed, 2),
                'metrics': self.heartbeat_metrics.copy(),
                **(logs or {})
            }
            
            payload = {
                'service_name': self.service_name,
                'service_type': 'scraper',
                'status': status,
                'last_activity': datetime.now().isoformat(),
                'logs': full_logs,
                'error_message': error_message,
                'metadata': metadata or {}
            }
            
            # Remove Content-Profile temporariamente para infra_actions (schema public)
            temp_headers = self.headers.copy()
            temp_headers.pop('Content-Profile', None)
            temp_headers.pop('Accept-Profile', None)
            
            url = f"{self.url}/rest/v1/infra_actions?on_conflict=service_name"
            r = self.session.post(url, json=[payload], headers=temp_headers, timeout=30)
            
            return r.status_code in (200, 201)
                
        except Exception as e:
            print(f"⚠️ Erro ao enviar heartbeat: {e}")
            return False
    
    def heartbeat_start(self, custom_logs: Optional[Dict] = None) -> bool:
        """Registra início da execução do scraper"""
        logs = {
            'event': 'start',
            'message': 'Scraper iniciado',
            **(custom_logs or {})
        }
        result = self._send_heartbeat(status='active', logs=logs)
        if result:
            print("💓 Heartbeat: Início registrado")
        return result
    
    def heartbeat_progress(self, items_processed: int = 0, pages_scraped: int = 0,
                          custom_logs: Optional[Dict] = None) -> bool:
        """Atualiza progresso durante execução"""
        self.heartbeat_metrics['items_processed'] += items_processed
        self.heartbeat_metrics['pages_scraped'] += pages_scraped
        
        logs = {
            'event': 'progress',
            'message': f"Processados {self.heartbeat_metrics['items_processed']} itens",
            **(custom_logs or {})
        }
        
        return self._send_heartbeat(status='active', logs=logs)
    
    def heartbeat_success(self, final_stats: Optional[Dict] = None) -> bool:
        """Registra conclusão com sucesso"""
        logs = {
            'event': 'completed',
            'message': 'Scraper concluído com sucesso',
            'final_stats': final_stats or {},
        }
        result = self._send_heartbeat(status='active', logs=logs)
        if result:
            print("💓 Heartbeat: Sucesso registrado")
        return result
    
    def heartbeat_error(self, error: Exception, context: Optional[str] = None) -> bool:
        """Registra erro durante execução"""
        self.heartbeat_metrics['errors'] += 1
        
        error_message = f"{type(error).__name__}: {str(error)}"
        if context:
            error_message = f"[{context}] {error_message}"
        
        logs = {
            'event': 'error',
            'error_type': type(error).__name__,
            'traceback': traceback.format_exc(),
            'context': context
        }
        
        result = self._send_heartbeat(
            status='error',
            logs=logs,
            error_message=error_message
        )
        if result:
            print("💓 Heartbeat: Erro registrado")
        return result
    
    def heartbeat_warning(self, message: str, details: Optional[Dict] = None) -> bool:
        """Registra warning"""
        self.heartbeat_metrics['warnings'] += 1
        
        logs = {
            'event': 'warning',
            'message': message,
            'details': details or {}
        }
        
        return self._send_heartbeat(status='warning', logs=logs)
    
    # ============================================
    # MÉTODOS ORIGINAIS MEGALEILOES
    # ============================================
    
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
        
        # URL com on_conflict para fazer UPSERT correto
        url = f"{self.url}/rest/v1/{self.table}?on_conflict=external_id"
        
        for i in range(0, len(prepared), batch_size):
            batch = prepared[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            
            try:
                r = self.session.post(url, json=batch, timeout=120)
                
                if r.status_code in (200, 201):
                    stats['inserted'] += len(batch)
                    print(f"  ✅ Batch {batch_num}/{total_batches}: {len(batch)} itens (insert/update)")
                    
                    # Atualiza heartbeat a cada batch
                    self.heartbeat_progress(
                        items_processed=len(batch),
                        custom_logs={'batch': batch_num, 'total_batches': total_batches}
                    )
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
        
        # Processa image_url
        image_url = item.get('image_url')
        if image_url and isinstance(image_url, str):
            image_url = image_url.strip()
            if not image_url or not image_url.startswith('http'):
                image_url = None
        else:
            image_url = None
        
        # Processa metadata
        metadata = item.get('metadata', {})
        if not isinstance(metadata, dict):
            metadata = {}
        
        # Monta item com todos os campos da tabela
        data = {
            'external_id': str(external_id),
            'category': str(item.get('category')) if item.get('category') else None,
            'title': str(item.get('title', 'Sem Título'))[:1000],
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
            'image_url': image_url,
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
    
    def get_with_images(self, limit: int = 100) -> List[Dict]:
        """Busca itens que possuem imagem"""
        try:
            url = f"{self.url}/rest/v1/{self.table}"
            params = {
                'image_url': 'not.is.null',
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
    
    def __del__(self):
        if hasattr(self, 'session'):
            self.session.close()


if __name__ == "__main__":
    # Teste do cliente
    print("🧪 Testando SupabaseMegaLeiloes com Heartbeat\n")
    
    client = SupabaseMegaLeiloes(service_name='test_scraper')
    
    if client.test():
        # Testa heartbeat
        print("\n💓 Testando Heartbeat:")
        client.heartbeat_start()
        time.sleep(2)
        
        client.heartbeat_progress(items_processed=100, pages_scraped=1)
        time.sleep(2)
        
        client.heartbeat_success(final_stats={'total': 100})
        
        # Estatísticas
        stats = client.get_stats()
        print(f"\n📊 Estatísticas:")
        print(f"  Total de itens: {stats['total']}")
        
        print("\n✅ Teste concluído!")
        print("\nVerifique as tabelas:")
        print("  - megaleiloes_items: dados dos leilões")
        print("  - infra_actions: heartbeat do scraper")