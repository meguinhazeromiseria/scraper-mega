#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NORMALIZER GENÉRICO - Normalização Universal de Dados de Leilões

✨ NOVIDADE: Extrai título LIMPO do external_id (MegaLeilões)
"""

import re
from typing import Dict, List, Optional


class UniversalNormalizer:
    """Normalizador genérico para TODOS os tipos de itens"""
    
    VALID_STATES = [
        'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
        'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN',
        'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO'
    ]
    
    def normalize(self, item: dict) -> dict:
        """
        Normaliza item para estrutura uniforme.
        """
        
        # ✨ EXTRAI TÍTULO LIMPO DO EXTERNAL_ID (para MegaLeilões)
        source = item.get('source', '').lower()
        external_id = item.get('external_id', '')
        
        if source == 'megaleiloes' and external_id:
            clean_title = self._extract_title_from_external_id(external_id)
        else:
            clean_title = self._clean_title(item.get('title'))
        
        return {
            # IDs
            'source': item.get('source'),
            'external_id': item.get('external_id'),
            
            # Título limpo (agora vem do external_id!)
            'title': clean_title,
            'normalized_title': self._normalize_for_search(clean_title),
            
            # Descrição limpa
            'description': self._clean_description(item.get('description')),
            'description_preview': self._create_preview(item.get('description'), clean_title),
            
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
            
            # Metadata
            'metadata': self._build_metadata(item),
        }
    
    def _extract_title_from_external_id(self, external_id: str) -> str:
        """
        ✨ NOVA FUNÇÃO - Extrai título limpo do external_id do MegaLeilões
        
        Exemplo:
        Input: "megaleiloes_sofa-em-estrutura-macica-tecido-de-veludo-j119233"
        Output: "Sofa Em Estrutura Macica Tecido De Veludo"
        
        Passos:
        1. Remove "megaleiloes_"
        2. Remove código do leilão (jXXXXXX no final)
        3. Substitui hífens por espaços
        4. Title Case
        """
        if not external_id:
            return "Sem título"
        
        # Remove prefixo "megaleiloes_"
        clean = external_id
        if clean.startswith('megaleiloes_'):
            clean = clean[len('megaleiloes_'):]
        
        # Remove código do leilão no final (padrão: -jXXXXXX ou -JXXXXXX)
        clean = re.sub(r'-j\d+$', '', clean, flags=re.IGNORECASE)
        
        # Substitui hífens e underscores por espaços
        clean = clean.replace('-', ' ').replace('_', ' ')
        
        # Remove espaços múltiplos
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Title Case
        clean = clean.title()
        
        # Limita tamanho
        if len(clean) > 200:
            clean = clean[:197] + '...'
        
        if not clean:
            return "Sem título"
        
        return clean
    
    def _clean_title(self, title: Optional[str]) -> str:
        """Limpa título tradicional (fallback para outros sites)"""
        if not title or not str(title).strip():
            return "Sem título"
        
        clean = str(title).strip()
        
        # Remove "LOTE XX" do início
        clean = re.sub(r'^LOTE\s+\d+\s*[-:—–]?\s*', '', clean, flags=re.IGNORECASE)
        
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', '', clean)
        
        # Remove vírgulas soltas no final
        clean = clean.rstrip(',').strip()
        
        # Remove "Placa FINAL X (UF)"
        clean = re.sub(r'\s*,?\s*Placa\s+FINAL\s+\d+\s*\([A-Z]{2}\)\s*,?', '', clean, flags=re.IGNORECASE)
        
        # Remove underscores e múltiplos espaços
        clean = clean.replace('_', ' ')
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        # Remove zeros à esquerda de números isolados
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
        
        # Remove acentos
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
        
        # Remove HTML tags
        desc = re.sub(r'<br\s*/?>', '\n', desc, flags=re.IGNORECASE)
        desc = re.sub(r'<[^>]+>', '', desc)
        
        # Remove múltiplas quebras de linha
        desc = re.sub(r'\n\s*\n+', '\n\n', desc)
        
        # Remove espaços múltiplos
        desc = re.sub(r' +', ' ', desc)
        
        # Limita tamanho
        if len(desc) > 3000:
            desc = desc[:2997] + '...'
        
        return desc.strip()
    
    def _create_preview(self, description: Optional[str], title: Optional[str]) -> str:
        """Cria preview curto da descrição"""
        if description:
            clean_desc = self._clean_description(description)
            if clean_desc:
                preview = clean_desc[:150].strip()
                if len(clean_desc) > 150:
                    preview += '...'
                return preview
        
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
        
        # Remove estado se vier junto
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
        
        if len(addr) > 255:
            addr = addr[:252] + '...'
        
        return addr
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Valida formato ISO de data"""
        if not date_str:
            return None
        
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
    
    def _build_metadata(self, item: dict) -> dict:
        """Constrói metadata preservando campos originais e extras"""
        metadata = item.get('metadata', {}).copy() if isinstance(item.get('metadata'), dict) else {}
        
        # Campos extras vão pro metadata
        extra_fields = [
            'vehicle_type', 'tech_category', 'tech_brand', 'tech_model',
            'tech_condition', 'tech_specs', 'property_type', 'area_m2',
            'bedrooms', 'bathrooms', 'quantity', 'unit_price',
            'condition', 'brand', 'model', 'year', 'raw_category'
        ]
        
        for field in extra_fields:
            if field in item and item[field] is not None:
                metadata[field] = item[field]
        
        return metadata


def normalize_items(items: List[dict]) -> List[dict]:
    """Normaliza lista de itens"""
    normalizer = UniversalNormalizer()
    return [normalizer.normalize(item) for item in items]


def normalize_item(item: dict) -> dict:
    """Normaliza um item único"""
    normalizer = UniversalNormalizer()
    return normalizer.normalize(item)


# ========== TESTE ==========
if __name__ == "__main__":
    print("\n🧪 TESTANDO NORMALIZER - Extração de Título do external_id\n")
    print("="*80)
    
    normalizer = UniversalNormalizer()
    
    test_items = [
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_sofa-em-estrutura-macica-tecido-de-veludo-fabricacao-propria-j119233',
            'title': '50% abaixo na 2ª praça R$ 3.500,00 262 0 Sofá em estrutura maciça...',
            'description': 'Sofá em veludo',
        },
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_cadeira-odontologica-completa-marca-kavo-modelo-unique-j119235',
            'title': '40% abaixo na 2ª praça R$ 5.000,00 229 0 Cadeira Odontológica completa...',
            'description': 'Cadeira odonto Kavo',
        },
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_armario-odontologico-de-06-modulos-j119239',
            'title': '40% abaixo na 2ª praça R$ 3.000,00 201 0 Armário Odontológico...',
            'description': 'Armário 6 módulos',
        },
        {
            'source': 'megaleiloes',
            'external_id': 'megaleiloes_servidores-dell-t300-e-powervault-md1000-j119127',
            'title': 'R$ 1,00 456 0 Servidores Dell - T300 e Powervault MD1000...',
            'description': 'Servidores',
        },
    ]
    
    for i, item in enumerate(test_items, 1):
        normalized = normalizer.normalize(item)
        
        print(f"\n{i}. ORIGINAL:")
        print(f"   external_id: {item['external_id']}")
        print(f"   title (sujo): {item['title'][:70]}...")
        
        print(f"\n   ✨ NORMALIZADO:")
        print(f"   title (limpo): {normalized['title']}")
        print(f"   normalized_title: {normalized['normalized_title']}")
        print("-" * 80)
    
    print("\n✅ Teste concluído!")