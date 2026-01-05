#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NORMALIZER GENÉRICO - Normalização Universal de Dados de Leilões

Uniformiza dados de QUALQUER categoria (veículos, tecnologia, móveis, etc)
para estrutura padrão do banco de dados.
"""

import re
from typing import Dict, List, Optional


class UniversalNormalizer:
    """Normalizador genérico para TODOS os tipos de itens"""
    
    # UFs válidos
    VALID_STATES = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    def normalize(self, item: dict) -> dict:
        """
        Normaliza item para estrutura uniforme.
        Funciona para QUALQUER categoria.
        """
        
        return {
            # IDs
            'source': item.get('source'),
            'external_id': item.get('external_id'),
            
            # Título limpo
            'title': self._clean_title(item.get('title')),
            'normalized_title': self._normalize_for_search(item.get('title')),
            
            # Descrição limpa
            'description': self._clean_description(item.get('description')),
            'description_preview': self._create_preview(item.get('description'), item.get('title')),
            
            # Valores
            'value': self._parse_value(item.get('value')),
            'value_text': item.get('value_text'),
            
            # Localização
            'city': self._clean_city(item.get('city')),
            'state': self._validate_state(item.get('state')),
            'address': self._clean_address(item.get('address')),
            
            # Leilão
            'auction_date': self._parse_date(item.get('auction_date')),
            'days_remaining': self._parse_days_remaining(item.get('days_remaining')),
            'auction_type': self._clean_text(item.get('auction_type'), 'Leilão'),
            'auction_name': self._clean_text(item.get('auction_name')),
            'store_name': self._clean_text(item.get('store_name')),
            'lot_number': self._clean_text(item.get('lot_number')),
            
            # Estatísticas
            'total_visits': self._parse_int(item.get('total_visits'), 0),
            'total_bids': self._parse_int(item.get('total_bids'), 0),
            'total_bidders': self._parse_int(item.get('total_bidders'), 0),
            
            # Link
            'link': item.get('link'),
            
            # Metadata original (preserva TUDO)
            'metadata': item.get('metadata', {}),
            
            # Campos extras (se existirem, passa direto)
            **self._extract_extra_fields(item)
        }
    
    def _clean_title(self, title: Optional[str]) -> str:
        """Limpa título removendo prefixos e formatação desnecessária"""
        if not title or not str(title).strip():
            return "Sem título"
        
        clean = str(title).strip()
        
        # Remove "LOTE XX" do início
        clean = re.sub(r'^LOTE\s+\d+\s*[-:–—]?\s*', '', clean, flags=re.IGNORECASE)
        
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', '', clean)
        
        # Remove vírgulas soltas no final
        clean = clean.rstrip(',').strip()
        
        # Remove "Placa FINAL X (UF)"
        clean = re.sub(r'\s*,?\s*Placa\s+FINAL\s+\d+\s*\([A-Z]{2}\)\s*,?', '', clean, flags=re.IGNORECASE)
        
        # Remove underscores e múltiplos espaços
        clean = clean.replace('_', ' ')
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Remove zeros à esquerda de números isolados (mas mantém "Fan 125", "03/2023")
        clean = re.sub(r'\b0+(\d{1,2})\b', r'\1', clean)
        
        # Limita tamanho
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean
    
    def _normalize_for_search(self, title: Optional[str]) -> str:
        """Normaliza título para busca (lowercase, sem acentos, sem pontuação)"""
        if not title:
            return ''
        
        normalized = str(title).lower()
        
        # Remove acentos (simplificado)
        replacements = {
            'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a',
            'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
            'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u', 'ü': 'u',
            'ç': 'c', 'ñ': 'n'
        }
        
        for old, new in replacements.items():
            normalized = normalized.replace(old, new)
        
        # Remove tudo que não é letra, número ou espaço
        normalized = re.sub(r'[^\w\s]', ' ', normalized)
        
        # Remove espaços múltiplos
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        return normalized
    
    def _clean_description(self, description: Optional[str]) -> Optional[str]:
        """Limpa descrição"""
        if not description:
            return None
        
        desc = str(description).strip()
        
        if not desc:
            return None
        
        # Remove HTML tags (substitui por quebra de linha)
        desc = re.sub(r'<br\s*/?>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<[^>]+>', '', desc)
        
        # Remove múltiplas quebras de linha
        desc = re.sub(r'\n\s*\n+', '\n\n', desc)
        
        # Remove espaços múltiplos
        desc = re.sub(r' +', ' ', desc)
        
        # Limita tamanho (3000 chars)
        if len(desc) > 3000:
            desc = desc[:2997] + '...'
        
        return desc.strip()
    
    def _create_preview(self, description: Optional[str], title: Optional[str]) -> str:
        """Cria preview curto da descrição"""
        # Tenta usar descrição limpa
        if description:
            clean_desc = self._clean_description(description)
            if clean_desc:
                preview = clean_desc[:150].strip()
                if len(clean_desc) > 150:
                    preview += '...'
                return preview
        
        # Fallback: usa título
        if title:
            return str(title)[:150]
        
        return "Sem descrição"
    
    def _parse_value(self, value) -> Optional[float]:
        """Normaliza valor monetário"""
        if value is None:
            return None
        
        try:
            val = float(value)
            if val < 0:
                return None
            return round(val, 2)
        except:
            return None
    
    def _clean_city(self, city: Optional[str]) -> Optional[str]:
        """Formata cidade (Title Case)"""
        if not city:
            return None
        
        city_clean = str(city).strip()
        
        if not city_clean:
            return None
        
        # Remove estado se vier junto (ex: "São Paulo/SP" -> "São Paulo")
        if '/' in city_clean:
            city_clean = city_clean.split('/')[0].strip()
        
        if '-' in city_clean:
            city_clean = city_clean.split('-')[0].strip()
        
        return city_clean.title()
    
    def _validate_state(self, state: Optional[str]) -> Optional[str]:
        """Valida UF"""
        if not state:
            return None
        
        state_clean = str(state).strip().upper()
        
        if state_clean in self.VALID_STATES:
            return state_clean
        
        return None
    
    def _clean_address(self, address: Optional[str]) -> Optional[str]:
        """Limpa endereço"""
        if not address:
            return None
        
        addr = str(address).strip()
        
        if not addr or len(addr) < 3:
            return None
        
        # Limita tamanho
        if len(addr) > 255:
            addr = addr[:252] + '...'
        
        return addr
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Valida formato ISO de data"""
        if not date_str:
            return None
        
        # Já está em formato ISO? Retorna
        if isinstance(date_str, str) and 'T' in date_str:
            return date_str
        
        return None
    
    def _parse_days_remaining(self, days) -> Optional[int]:
        """Parse dias restantes"""
        if days is None:
            return None
        
        try:
            days_int = int(days)
            if days_int < 0:
                return 0
            return days_int
        except:
            return None
    
    def _clean_text(self, text: Optional[str], default: Optional[str] = None) -> Optional[str]:
        """Limpa texto genérico"""
        if not text:
            return default
        
        clean = str(text).strip()
        
        if not clean:
            return default
        
        # Limita tamanho
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        return clean
    
    def _parse_int(self, value, default: int = 0) -> int:
        """Parse inteiro com default"""
        if value is None:
            return default
        
        try:
            return int(value)
        except:
            return default
    
    def _extract_extra_fields(self, item: dict) -> dict:
        """Extrai campos extras que podem existir (vehicle_type, tech_category, etc)"""
        extra_fields = {}
        
        # Lista de campos opcionais conhecidos
        optional_fields = [
            'vehicle_type',      # veículos
            'tech_category',     # tecnologia
            'tech_brand',
            'tech_model',
            'tech_condition',
            'tech_specs',
            'property_type',     # imóveis
            'area_m2',
            'bedrooms',
            'bathrooms',
            'quantity',          # lotes
            'unit_price',
            'condition',         # genérico
            'brand',
            'model',
            'year',
            'raw_category',      # categoria original do site
        ]
        
        for field in optional_fields:
            if field in item and item[field] is not None:
                extra_fields[field] = item[field]
        
        return extra_fields


def normalize_items(items: List[dict]) -> List[dict]:
    """Normaliza lista de itens"""
    normalizer = UniversalNormalizer()
    return [normalizer.normalize(item) for item in items]


# Função auxiliar para uso direto
def normalize_item(item: dict) -> dict:
    """Normaliza um item único"""
    normalizer = UniversalNormalizer()
    return normalizer.normalize(item)