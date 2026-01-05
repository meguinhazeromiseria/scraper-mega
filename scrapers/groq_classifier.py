#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER - Classificador Inteligente de Tabelas
🤖 Groq AI como CÉREBRO principal - mínimo de regex
✨ Versão ULTRA-INTELIGENTE
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
    """Classificador que CONFIA no Groq para 99% das decisões"""
    
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
        ÚNICO bloqueio pré-Groq: itens financeiros/abstratos.
        Estes não têm categoria física, então sempre vão para diversos.
        """
        text = f"{item.get('normalized_title', '')} {item.get('description', '')}".lower()
        return any(kw in text for kw in FINANCIAL_ABSTRACT_KEYWORDS)
    
    def _is_obvious_mixed_lot(self, item: Dict) -> bool:
        """
        Detecta APENAS lotes MUITO ÓBVIOS no título.
        Ex: "TVs, Geladeiras, Micro-ondas, Bebedouro e Telefone"
        """
        title = item.get('normalized_title', '').lower()
        
        # Detecta múltiplos itens separados por vírgula (3+)
        if not re.search(r'\w+\s*,\s*\w+.*,\s*\w+', title):
            return False
        
        # Verifica se são categorias MUITO diferentes
        categories_found = set()
        
        for category, indicators in MIXED_LOT_CATEGORY_INDICATORS.items():
            if any(indicator in title for indicator in indicators):
                categories_found.add(category)
        
        return len(categories_found) >= 2
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica um item - GROQ FAZ QUASE TUDO.
        
        Fluxo ULTRA-SIMPLIFICADO:
        1. Bloqueia financeiros → diversos
        2. Detecta mistos óbvios → diversos
        3. GROQ DECIDE TUDO → categoria específica ou diversos
        4. Fallback → diversos
        """
        title = item.get('normalized_title', '').strip()
        description = item.get('description', '')[:500]
        
        if not title:
            self.stats['failed'] += 1
            self.stats['total'] += 1
            return None
        
        # 1️⃣ BLOQUEIA FINANCEIROS/ABSTRATOS (único caso óbvio)
        if self._is_financial_abstract(item):
            self.stats['financial_blocked'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            if self.stats['financial_blocked'] <= 3:
                print(f"  💼 DIVERSOS (financeiro): '{title[:60]}'")
            
            return 'diversos'
        
        # 2️⃣ DETECTA MISTOS MUITO ÓBVIOS (ex: "TVs, geladeiras, telefones")
        if self._is_obvious_mixed_lot(item):
            self.stats['mixed_detected'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            if self.stats['mixed_detected'] <= 3:
                print(f"  🎨 DIVERSOS (misto): '{title[:60]}'")
            
            return 'diversos'
        
        # 3️⃣ GROQ DECIDE (99% dos casos)
        table_name = self._classify_with_groq(title, description)
        
        if table_name:
            self.stats['groq_classifications'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            
            if self.stats['groq_classifications'] <= 10:
                print(f"  🤖 {table_name}: '{title[:55]}'")
            
            return table_name
        
        # 4️⃣ FALLBACK (se Groq falhar completamente)
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """Classifica com Groq - prompt MUITO melhorado"""
        prompt = self._build_ultra_smart_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if response:
                response_clean = response.strip().lower()
                
                # Remove lixo
                if '\n' in response_clean:
                    response_clean = response_clean.split('\n')[0]
                
                response_clean = response_clean.replace(',', '').replace(';', '').strip()
                
                # Valida se é tabela válida
                if response_clean in TABLES_INFO:
                    return response_clean
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro Groq: {e}")
            return None
    
    def _build_ultra_smart_prompt(self, title: str, description: str) -> str:
        """
        Prompt ULTRA-DETALHADO com exemplos concretos.
        O Groq precisa entender CONTEXTO, não apenas keywords.
        """
        
        tables_list = "\n".join([
            f"- {table}: {info['desc']}"
            for table, info in TABLES_INFO.items()
        ])
        
        prompt = f"""Você é um especialista em classificação de leilões. Analise o item abaixo e escolha a categoria MAIS ESPECÍFICA baseando-se no CONTEXTO e USO REAL do item.

CATEGORIAS DISPONÍVEIS:
{tables_list}

ITEM PARA CLASSIFICAR:
Título: {title}
Descrição: {description[:400] if description else 'N/A'}

====================================
REGRAS DE CLASSIFICAÇÃO (DETALHADAS)
====================================

🥼 PRIORIDADE 1 - NICHADOS (equipamentos profissionais especializados):

A) SAÚDE/FARMÁCIA:
   ✅ Medicamentos, vitaminas, produtos de higiene HOSPITALAR → "nichados"
   ✅ Lotes de farmácia, drogaria, produtos de saúde → "nichados"
   ✅ Equipamentos médicos, odontológicos, veterinários → "nichados"
   
   Exemplos:
   - "Medicamentos, produtos de higiene, vitaminas" → nichados
   - "Lote com 2.333 itens de medicamentos e saúde" → nichados
   - "Cadeira odontológica Kavo" → nichados
   - "Armário odontológico 6 módulos" → nichados
   
B) COZINHA INDUSTRIAL:
   ✅ Fogão INDUSTRIAL, geladeira INDUSTRIAL → "nichados"
   ✅ Equipamento com "6 bocas", "inox profissional" → "nichados"
   ❌ Fogão doméstico comum → "eletrodomesticos"
   
   Exemplos:
   - "Fogão industrial 6 bocas inox" → nichados
   - "Geladeira industrial câmara fria" → nichados
   - "Fogão 4 bocas Brastemp" → eletrodomesticos

C) OUTROS NICHADOS:
   - Equipamento veterinário, estética, laboratório → "nichados"

---

🏗️ PRIORIDADE 2 - CONSTRUÇÃO vs INDUSTRIAL:

A) MATERIAIS_CONSTRUCAO:
   ✅ Máquinas para CORTAR/CONSTRUIR materiais → "materiais_construcao"
   ✅ Ferramentas de construção civil → "materiais_construcao"
   
   Exemplos:
   - "Cortadeira de piso de bancada" → materiais_construcao
   - "Serra mármore" → materiais_construcao
   - "Disco de corte" → materiais_construcao

B) INDUSTRIAL_EQUIPAMENTOS:
   ✅ Máquinas de PRODUÇÃO em série → "industrial_equipamentos"
   ✅ Torno, fresadora, prensa, CNC → "industrial_equipamentos"
   
   Exemplos:
   - "Torno mecânico industrial" → industrial_equipamentos
   - "Prensa hidráulica" → industrial_equipamentos

---

💻 PRIORIDADE 3 - TECNOLOGIA vs ELETRODOMÉSTICOS:

A) TECNOLOGIA:
   ✅ Informática, comunicação, impressão → "tecnologia"
   
   Exemplos:
   - "19 impressoras portáteis Tekpix" → tecnologia
   - "Impressora digital com tecnologia ZINK" → tecnologia
   - "Notebook, celular, tablet, servidor" → tecnologia

B) ELETRODOMESTICOS:
   ✅ Linha branca doméstica, TV, microondas → "eletrodomesticos"
   
   Exemplos:
   - "Geladeira Brastemp" → eletrodomesticos
   - "TV LED 50 polegadas" → eletrodomesticos
   - "Microondas Electrolux" → eletrodomesticos

---

🪑 PRIORIDADE 4 - MÓVEIS vs UTILIDADES:

A) MOVEIS_DECORACAO:
   ✅ Móvel = você SENTA, GUARDA coisas, DECORA
   
   Exemplos:
   - "Sofá, mesa, cadeira, armário, estante" → moveis_decoracao
   - "Móveis de escritório" → moveis_decoracao
   - "Cadeira de escritório giratória" → moveis_decoracao

B) CASA_UTILIDADES:
   ✅ Utensílio = você USA para cozinhar/comer/limpar
   
   Exemplos:
   - "Panela, prato, copo, talher" → casa_utilidades
   - "Kit churrasco" → casa_utilidades

---

🎨 DIVERSOS - APENAS QUANDO:

1. Lote EXPLICITAMENTE misto com múltiplas categorias diferentes
2. Item que NÃO se encaixa em NENHUMA categoria acima
3. Lote com palavras "itens diversos", "produtos variados", "lote misto"

Exemplos:
- "TVs, geladeiras, micro-ondas, bebedouro e telefone" → diversos (múltiplas categorias)
- "Lote variado de produtos" → diversos

====================================

RESPONDA APENAS O NOME DA CATEGORIA (ex: "tecnologia", "nichados", "diversos"):"""
        
        return prompt
    
    def _call_groq(self, prompt: str) -> Optional[str]:
        """Chama API Groq com configuração otimizada"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um classificador EXPERT em leilões com 20 anos de experiência. Analise o CONTEXTO completo e a FUNÇÃO REAL do item. Use bom senso profissional, não apenas palavras-chave. Responda APENAS o nome da categoria."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.05,  # MUITO determinístico
            "max_tokens": 50,
            "top_p": 0.85
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
        print(f"🤖 Groq (IA): {self.stats['groq_classifications']} ({self.stats['groq_classifications']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"💼 Financeiros bloqueados: {self.stats['financial_blocked']} ({self.stats['financial_blocked']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"🎨 Mistos detectados: {self.stats['mixed_detected']} ({self.stats['mixed_detected']/max(self.stats['total'],1)*100:.1f}%)")
        
        if self.stats['by_table']:
            print(f"\n📦 DISTRIBUIÇÃO POR TABELA:")
            print("-" * 80)
            
            for table, count in sorted(self.stats['by_table'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['total'] * 100
                bar = "█" * int(pct / 2)
                emoji = "🎨" if table == 'diversos' else "  "
                print(f"{emoji} {table:.<35} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)
        print(f"\n💡 Groq está fazendo {self.stats['groq_classifications']/max(self.stats['total'],1)*100:.1f}% do trabalho")
        print(f"   (quanto mais próximo de 100%, melhor!)")


def classify_item_to_table(item: Dict) -> str:
    """Classifica um item"""
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    print("\n🤖 TESTE - CLASSIFICADOR ULTRA-INTELIGENTE\n")
    print("="*80)
    print("Groq como CÉREBRO - mínimo de regex")
    print("="*80 + "\n")
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # ========================================
        # DIVERSOS - CASOS QUE VOCÊ REPORTOU:
        # ========================================
        
        # 1. FINANCEIROS/ABSTRATOS → diversos
        {
            "normalized_title": "creditos-de-emprestimo-compulsorio-sobre-consumo-de-energia-eletrica",
            "description": "Créditos de Empréstimo Compulsório sobre Consumo de Energia Elétrica"
        },
        {
            "normalized_title": "registros-de-marca-lock-e-athol",
            "description": "Registros de Marca - LOCK e ATHOL"
        },
        {
            "normalized_title": "expectativa-de-direitos-creditorios-contra-a-vale-sa",
            "description": "Expectativa de Direitos Creditórios contra a VALE S.A."
        },
        {
            "normalized_title": "marca-regenfill-devidamente-registrada-no-inpi-servicos",
            "description": "Marca REGENFILL devidamente registrada no INPI"
        },
        {
            "normalized_title": "5948-acoes-preferenciais-classe-b-elet6-da-eletrobras",
            "description": "5.948 Ações Preferenciais Classe B (ELET6) da Eletrobrás"
        },
        {
            "normalized_title": "titulo-patrimonial-do-club-athletico-paulistano",
            "description": "Título Patrimonial do Club Athletico Paulistano"
        },
        {
            "normalized_title": "cotas-sociais-de-empresas-edilson-vila-e-edith-figueiredo",
            "description": "Cotas Sociais de Empresas"
        },
        
        # 2. LOTE MISTO → diversos
        {
            "normalized_title": "tvs-geladeiras-micro-ondas-bebedouro-e-telefone",
            "description": "TVs, Geladeiras, Micro-ondas, Bebedouro e Telefone"
        },
        
        # ========================================
        # CASOS ESPECÍFICOS (não são diversos):
        # ========================================
        
        # MEDICAMENTOS → nichados
        {
            "normalized_title": "medicamentos-produtos-de-higiene-vitaminas-e-demais-itens-correlatos",
            "description": "Medicamentos, produtos de higiene, vitaminas e demais itens correlatos"
        },
        {
            "normalized_title": "lote-com-2333-itens-de-medicamentos-saude-higiene-cosmeticos-e-perfumaria",
            "description": "Lote com 2.333 Itens de Medicamentos, Saúde, Higiene"
        },
        
        # COMPACTADOR → industrial_equipamentos (NÃO casa_utilidades!)
        {
            "normalized_title": "01-compactador-de-lixo-e-coletor-rodotec-capacidade-15m",
            "description": "01 Compactador de lixo e coletor Rodotec, capacidade 15m"
        },
        
        # CONSTRUÇÃO → materiais_construcao
        {
            "normalized_title": "maquina-cortadeira-de-piso-de-bancada-cortag",
            "description": "Máquina Cortadeira de Piso de Bancada, CORTAG"
        },
        
        # TECNOLOGIA → tecnologia (NÃO materiais_construcao!)
        {
            "normalized_title": "19-impressoras-digitais-portateis-tekpix-com-tecnologia-zink",
            "description": "19 Impressoras Digitais Portáteis Tekpix com Tecnologia ZINK"
        },
        
        # MÓVEIS → moveis_decoracao
        {
            "normalized_title": "sofa-em-estrutura-macica-tecido-veludo",
            "description": "Sofá em estrutura maciça tecido de veludo"
        },
        
        # ODONTO → nichados
        {
            "normalized_title": "cadeira-odontologica-completa-marca-kavo",
            "description": "Cadeira odontológica Kavo completa"
        },
    ]
    
    print("🔍 CLASSIFICANDO OS CASOS PROBLEMÁTICOS...\n")
    
    print("=" * 80)
    print("ESPERADO: DIVERSOS (financeiros/abstratos + lote misto)")
    print("=" * 80)
    
    for i in range(8):  # Primeiros 8 são diversos
        item = test_items[i]
        table = classifier.classify(item)
        status = "✅" if table == "diversos" else "❌"
        print(f"{status} {i+1}. '{item['normalized_title'][:55]}'")
        print(f"     → {table}\n")
    
    print("=" * 80)
    print("ESPERADO: CATEGORIAS ESPECÍFICAS (não diversos)")
    print("=" * 80)
    
    for i in range(8, len(test_items)):  # Resto são categorias específicas
        item = test_items[i]
        table = classifier.classify(item)
        status = "✅" if table != "diversos" else "❌"
        print(f"{status} {i+1}. '{item['normalized_title'][:55]}'")
        print(f"     → {table}\n")
    
    classifier.print_stats()