#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER - Classificador Inteligente de Tabelas
🤖 Usa Groq AI para decidir em qual tabela cada item deve ser inserido
✨ Versão refatorada - MENOS regex, MAIS inteligência
"""

import json
import requests
import os
import re
from typing import Optional, Dict, List
from dotenv import load_dotenv
from category_indicators import (
    TABLES_INFO,
    MIXED_LOT_CATEGORY_INDICATORS,
    FINANCIAL_ABSTRACT_KEYWORDS
)

load_dotenv()

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqTableClassifier:
    """Classificador que USA Groq para quase tudo"""
    
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.api_url = GROQ_API_URL
        
        if not self.api_key:
            raise ValueError("⚠️ GROQ_API_KEY não encontrada! Configure no .env")
        
        self.stats = {
            'total': 0,
            'groq_classifications': 0,
            'financial_blocked': 0,
            'mixed_detected': 0,
            'failed': 0,
            'by_table': {}
        }
    
    def _is_financial_abstract(self, item: Dict) -> bool:
        """
        Detecta itens FINANCEIROS/ABSTRATOS que devem ir para diversos.
        ÚNICO filtro pré-Groq que bloqueia classificação.
        """
        text = f"{item.get('normalized_title', '')} {item.get('description', '')}".lower()
        return any(kw in text for kw in FINANCIAL_ABSTRACT_KEYWORDS)
    
    def _is_obvious_mixed_lot(self, item: Dict) -> bool:
        """
        Detecta lotes OBVIAMENTE mistos no título.
        Ex: "TVs, Geladeiras, Micro-ondas, Bebedouro e Telefone"
        """
        title = item.get('normalized_title', '').lower()
        
        # Detecta múltiplos itens separados por vírgula
        if not re.search(r'\w+\s*,\s*\w+.*,\s*\w+', title):
            return False
        
        # Verifica se são categorias diferentes
        categories_found = set()
        
        for category, indicators in MIXED_LOT_CATEGORY_INDICATORS.items():
            if any(indicator in title for indicator in indicators):
                categories_found.add(category)
        
        return len(categories_found) >= 2
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica um item e retorna o nome da tabela.
        
        Fluxo SIMPLIFICADO:
        1. Verifica se é financeiro/abstrato → diversos
        2. Verifica se é lote misto óbvio → diversos
        3. USA GROQ para TUDO o resto
        4. Fallback → diversos (se Groq falhar)
        """
        title = item.get('normalized_title', '').strip()
        description = item.get('description', '')[:500]
        
        if not title:
            self.stats['failed'] += 1
            self.stats['total'] += 1
            return None
        
        # 1️⃣ BLOQUEIA FINANCEIROS/ABSTRATOS
        if self._is_financial_abstract(item):
            self.stats['financial_blocked'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            if self.stats['financial_blocked'] <= 3:
                print(f"  💼 DIVERSOS (financeiro): '{title[:60]}'")
            
            return 'diversos'
        
        # 2️⃣ DETECTA LOTES MISTOS ÓBVIOS
        if self._is_obvious_mixed_lot(item):
            self.stats['mixed_detected'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            if self.stats['mixed_detected'] <= 3:
                print(f"  🎨 DIVERSOS (misto): '{title[:60]}'")
            
            return 'diversos'
        
        # 3️⃣ DELEGA TUDO PARA O GROQ
        table_name = self._classify_with_groq(title, description)
        
        if table_name and table_name != 'diversos':
            self.stats['groq_classifications'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            
            if self.stats['groq_classifications'] <= 8:
                print(f"  🤖 {table_name}: '{title[:55]}'")
            
            return table_name
        
        # 4️⃣ FALLBACK (se Groq falhar)
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """Classifica com Groq - agora com prompt MELHORADO"""
        prompt = self._build_smart_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if response:
                response_clean = response.strip().lower()
                
                if '\n' in response_clean:
                    response_clean = response_clean.split('\n')[0]
                
                response_clean = response_clean.replace(',', '').replace(';', '').strip()
                
                if response_clean in TABLES_INFO:
                    return response_clean
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro Groq: {e}")
            return None
    
    def _build_smart_prompt(self, title: str, description: str) -> str:
        """
        Prompt INTELIGENTE para Groq.
        Foca em EXEMPLOS ao invés de keywords.
        """
        
        tables_list = "\n".join([
            f"- {table}: {info['desc']}"
            for table, info in TABLES_INFO.items()
        ])
        
        prompt = f"""Você é um classificador de leilões. Identifique a categoria MAIS ESPECÍFICA baseando-se no CONTEXTO e FUNÇÃO do item.

CATEGORIAS DISPONÍVEIS:
{tables_list}

ITEM PARA CLASSIFICAR:
Título: {title}
Descrição: {description[:300] if description else 'N/A'}

REGRAS DE DECISÃO (use BOM SENSO, não apenas palavras-chave):

🔍 PRIORIDADE 1 - ESPECIALIDADES (nichados):
- Equipamento de consultório médico/odontológico → "nichados"
  Ex: cadeira odontológica, raio-x dental, autoclave, maca
- Equipamento de cozinha INDUSTRIAL/PROFISSIONAL → "nichados"
  Ex: fogão industrial 6 bocas, geladeira industrial, forno combinado
- Equipamento veterinário, estética, laboratório → "nichados"

🏠 PRIORIDADE 2 - MÓVEIS vs UTILIDADES:
- Móvel é algo em que você SENTA, GUARDA coisas, ou DECORA → "moveis_decoracao"
  Ex: sofá, mesa, cadeira, armário, estante, rack, cama
- Utensílio é algo que você USA para cozinhar/comer → "casa_utilidades"
  Ex: panela, prato, copo, talher

💻 PRIORIDADE 3 - TECNOLOGIA vs ELETRODOMÉSTICOS:
- TECNOLOGIA = informática, comunicação, entretenimento portátil
  Ex: notebook, celular, tablet, impressora, servidor, console
- ELETRODOMÉSTICOS = linha branca, conforto doméstico
  Ex: geladeira doméstica, fogão doméstico, TV, microondas, air fryer

🏗️ PRIORIDADE 4 - CONSTRUÇÃO:
- Ferramenta/máquina para CONSTRUIR/CORTAR → "materiais_construcao"
  Ex: cortadeira de piso, serra mármore, disco de corte
- Material BRUTO → "materiais_construcao"
  Ex: cimento, tijolo, tinta

🚗 PRIORIDADE 5 - VEÍCULOS:
- QUALQUER coisa que TRANSPORTA pessoas/carga → "veiculos"
  Ex: carro, moto, bicicleta, caminhão, patinete

⚠️ DIVERSOS - apenas para:
- Itens explicitamente descritos como "lote misto"
- OU quando o item NÃO se encaixa em NENHUMA categoria acima

RESPONDA APENAS O NOME DA CATEGORIA (ex: "tecnologia", "moveis_decoracao", etc):"""
        
        return prompt
    
    def _call_groq(self, prompt: str) -> Optional[str]:
        """Chama API Groq"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um classificador expert em leilões. Use bom senso e contexto, não apenas palavras-chave. Responda APENAS o nome da categoria."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,  # Menos aleatório
            "max_tokens": 50,
            "top_p": 0.9
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('choices') and len(data['choices']) > 0:
                    return data['choices'][0]['message']['content'].strip()
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro chamada Groq: {e}")
            return None
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas"""
        return self.stats.copy()
    
    def print_stats(self):
        """Imprime estatísticas"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS DE CLASSIFICAÇÃO")
        print("="*80)
        print(f"Total: {self.stats['total']}")
        print(f"Groq (IA): {self.stats['groq_classifications']} ({self.stats['groq_classifications']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Financeiros bloqueados: {self.stats['financial_blocked']} ({self.stats['financial_blocked']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Mistos detectados: {self.stats['mixed_detected']} ({self.stats['mixed_detected']/max(self.stats['total'],1)*100:.1f}%)")
        
        if self.stats['by_table']:
            print(f"\n📦 DISTRIBUIÇÃO:")
            print("-" * 80)
            
            for table, count in sorted(self.stats['by_table'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['total'] * 100
                bar = "█" * int(pct / 2)
                emoji = "🎨" if table == 'diversos' else "  "
                print(f"{emoji} {table:.<35} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)


def classify_item_to_table(item: Dict) -> str:
    """Classifica um item"""
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    print("\n🤖 TESTE - CLASSIFICADOR INTELIGENTE (mais Groq, menos regex)\n")
    print("="*80)
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # DIVERSOS - FINANCEIROS (bloqueio pré-Groq)
        {"normalized_title": "cotas-sociais-de-empresas-edilson-vila-e-edith-figueiredo", "description": "Cotas Sociais de Empresas"},
        {"normalized_title": "5948-acoes-preferenciais-classe-b-elet6-da-eletrobras", "description": "Ações Preferenciais Eletrobrás"},
        
        # DIVERSOS - LOTE MISTO (detecção pré-Groq)
        {"normalized_title": "tvs-geladeiras-micro-ondas-bebedouro-e-telefone", "description": "TVs, Geladeiras, Micro-ondas"},
        
        # GROQ DEVE CLASSIFICAR (casos que precisam inteligência):
        {"normalized_title": "19-impressoras-digitais-portateis-tekpix", "description": "Impressoras portáteis com tecnologia ZINK"},
        {"normalized_title": "maquina-cortadeira-de-piso-de-bancada-cortag", "description": "Cortadeira de piso bancada"},
        {"normalized_title": "cadeira-odontologica-completa-marca-kavo", "description": "Cadeira odontológica Kavo"},
        {"normalized_title": "armario-odontologico-de-06-modulos", "description": "Armário consultório odonto"},
        {"normalized_title": "fogao-industrial-6-bocas-inox", "description": "Fogão industrial 6 bocas"},
        {"normalized_title": "sofa-em-estrutura-macica-tecido-veludo", "description": "Sofá veludo"},
        {"normalized_title": "moveis-de-escritorio-mesa-cadeira", "description": "Móveis escritório"},
        {"normalized_title": "servidores-dell-t300-e-powervault", "description": "Servidores Dell"},
        {"normalized_title": "aparelho-celular-moto-g-22", "description": "Celular Moto G"},
    ]
    
    print("🔍 CLASSIFICANDO COM GROQ...\n")
    
    for item in test_items:
        table = classifier.classify(item)
        print(f"'{item['normalized_title'][:60]}'")
        print(f"  → {table}\n")
    
    classifier.print_stats()
    
    print("\n💡 ANÁLISE:")
    groq_pct = classifier.stats['groq_classifications'] / max(classifier.stats['total'], 1) * 100
    print(f"Groq está fazendo {groq_pct:.1f}% do trabalho (quanto mais, melhor!)")
    print(f"Bloqueios pré-Groq: {classifier.stats['financial_blocked'] + classifier.stats['mixed_detected']} (apenas casos óbvios)")