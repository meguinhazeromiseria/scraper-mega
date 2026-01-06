#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER v3.0 - 100% Powered by Groq AI
🤖 Llama 3.3 70B Versatile - Zero keywords, full intelligence
✨ Classifica em todas as 17 tabelas com raciocínio contextual
"""

import json
import requests
import os
from typing import Optional, Dict
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqTableClassifier:
    """
    Classificador 100% Groq AI - Zero keywords, full context understanding.
    Llama 3.3 70B analisa contexto, função e características para decidir.
    """
    
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.api_url = GROQ_API_URL
        
        if not self.api_key:
            raise ValueError("⚠️ GROQ_API_KEY não encontrada! Configure no .env")
        
        self.stats = {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'by_table': {}
        }
        
        # Tabelas válidas (17 categorias)
        self.valid_tables = {
            'tecnologia', 'veiculos', 'eletrodomesticos', 'bens_consumo',
            'moveis_decoracao', 'casa_utilidades', 'alimentos_bebidas',
            'artes_colecionismo', 'imoveis', 'materiais_construcao',
            'industrial_equipamentos', 'maquinas_pesadas_agricolas',
            'nichados', 'partes_pecas', 'animais', 'sucatas_residuos', 'diversos'
        }
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica item usando 100% Groq AI.
        
        Args:
            item: Dict com 'normalized_title' e opcionalmente 'description'
        
        Returns:
            Nome da tabela ou None se falhar
        """
        title = item.get('normalized_title', '').strip()
        description = item.get('description', '')[:800]  # Mais contexto
        
        if not title:
            self.stats['failed'] += 1
            self.stats['total'] += 1
            return None
        
        # Chama Groq AI
        table_name = self._classify_with_groq(title, description)
        
        if table_name and table_name in self.valid_tables:
            self.stats['successful'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            
            # Log progressivo
            if self.stats['total'] <= 20 or self.stats['total'] % 100 == 0:
                print(f"  🤖 [{self.stats['total']:>4}] {table_name:.<30} '{title[:45]}'")
            
            return table_name
        
        # Fallback
        self.stats['failed'] += 1
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        
        if self.stats['failed'] <= 5:
            print(f"  ⚠️  FALLBACK diversos: '{title[:50]}'")
        
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """
        Classifica usando Groq AI com prompt otimizado para Llama 3.3 70B.
        
        Best practices aplicadas:
        - Temperature 0.2 (balanceado para classificação)
        - Sistema de instruções claro
        - Few-shot examples
        - Contexto estruturado
        """
        prompt = self._build_optimized_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if not response:
                return None
            
            # Limpa e valida resposta
            category = self._extract_category(response)
            return category if category in self.valid_tables else None
        
        except Exception as e:
            if self.stats['failed'] <= 3:
                print(f"⚠️ Erro Groq: {e}")
            return None
    
    def _build_optimized_prompt(self, title: str, description: str) -> str:
        """
        Prompt otimizado para Llama 3.3 70B com few-shot examples.
        Foca em raciocínio contextual e função do item.
        """
        
        prompt = f"""Você é um classificador especialista de itens de leilão. Analise o contexto, função e características do item para determinar a categoria MAIS ESPECÍFICA possível.

📦 ITEM PARA CLASSIFICAR:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Título: {title}
Descrição: {description if description else 'N/A'}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 CATEGORIAS DISPONÍVEIS (17 opções):

🏠 GRANDES ATIVOS
├─ imoveis → propriedades físicas (casa, apartamento, terreno, lote, sala comercial, galpão, fazenda)
└─ veiculos → meios de transporte (carro, moto, caminhão, ônibus, barco, avião, bicicleta)

💻 TECNOLOGIA & ELETRÔNICOS
├─ tecnologia → informática e comunicação (notebook, celular, tablet, impressora, câmera, drone, servidor)
└─ eletrodomesticos → linha branca e entretenimento doméstico (geladeira, fogão, TV, ar condicionado, microondas)

🛋️ CASA & DECORAÇÃO
├─ moveis_decoracao → mobília e decoração (sofá, mesa, cadeira, armário, cama, estante, lustre, quadros)
├─ casa_utilidades → utensílios domésticos (panela, prato, copo, talher, organizador, vassoura)
└─ artes_colecionismo → obras de arte, antiguidades, colecionáveis raros

🛍️ CONSUMO
├─ bens_consumo → itens pessoais (roupas, calçados, bolsas, óculos, relógios, joias, perfumes)
└─ alimentos_bebidas → produtos alimentícios e bebidas (vinho, café, suplementos)

🏗️ CONSTRUÇÃO & INDUSTRIAL
├─ materiais_construcao → insumos de obra (cimento, tijolo, piso, tinta, ferramentas de construção)
├─ industrial_equipamentos → maquinário industrial (torno, prensa, compressor, gerador, compactador)
└─ maquinas_pesadas_agricolas → equipamentos pesados (trator, escavadeira, colheitadeira, retroescavadeira)

🏥 ESPECIALIDADES
└─ nichados → equipamentos profissionais especializados (médico, odontológico, farmácia, veterinário, cozinha industrial, estética profissional)

🔧 OUTROS
├─ partes_pecas → componentes avulsos, peças de reposição, sobressalentes
├─ animais → animais vivos (gado, cavalos, aves)
└─ sucatas_residuos → materiais para reciclagem, sucata, descarte

🎨 CATCH-ALL
└─ diversos → APENAS para: (1) itens abstratos (ações, créditos, marcas, direitos, patentes) OU (2) lotes explicitamente mistos com múltiplas categorias muito diferentes

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📖 EXEMPLOS DE CLASSIFICAÇÃO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXEMPLO 1:
Título: "Apartamento 53 m² com 1 vaga - Parque das Nações"
→ Categoria: imoveis
Raciocínio: Propriedade imobiliária residencial

EXEMPLO 2:
Título: "Carro Volkswagen Gol 1.0 2015"
→ Categoria: veiculos
Raciocínio: Veículo automotor completo

EXEMPLO 3:
Título: "Fogão Industrial 6 Bocas em Inox - Metalúrgica"
→ Categoria: nichados
Raciocínio: Equipamento de cozinha profissional/industrial, não doméstico

EXEMPLO 4:
Título: "Notebook Dell i5 8GB RAM"
→ Categoria: tecnologia
Raciocínio: Equipamento de informática

EXEMPLO 5:
Título: "Sofá 3 Lugares + Poltrona Estofada"
→ Categoria: moveis_decoracao
Raciocínio: Mobília residencial

EXEMPLO 6:
Título: "Conjunto de Panelas 10 Peças Tramontina"
→ Categoria: casa_utilidades
Raciocínio: Utensílios de cozinha doméstica

EXEMPLO 7:
Título: "Trator Agrícola John Deere 75HP"
→ Categoria: maquinas_pesadas_agricolas
Raciocínio: Maquinário agrícola pesado

EXEMPLO 8:
Título: "Compressor de Ar Industrial 20HP"
→ Categoria: industrial_equipamentos
Raciocínio: Equipamento industrial de produção

EXEMPLO 9:
Título: "Cadeira Odontológica Kavo + Equipo Completo"
→ Categoria: nichados
Raciocínio: Equipamento odontológico profissional

EXEMPLO 10:
Título: "Motor de Arranque para VW Gol (peça)"
→ Categoria: partes_pecas
Raciocínio: Componente avulso de reposição

EXEMPLO 11:
Título: "10 Cabeças de Gado Nelore"
→ Categoria: animais
Raciocínio: Animais vivos

EXEMPLO 12:
Título: "Lote: TV, Geladeira, Micro-ondas, Sofá, Mesa"
→ Categoria: diversos
Raciocínio: Lote misto com categorias muito diferentes (tecnologia + eletrodomésticos + móveis)

EXEMPLO 13:
Título: "1.000 ações preferenciais Petrobras"
→ Categoria: diversos
Raciocínio: Ativo financeiro abstrato

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🎯 INSTRUÇÕES DE CLASSIFICAÇÃO:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. ANALISE O CONTEXTO: Qual é a FUNÇÃO PRINCIPAL e USO REAL do item?
2. PRIORIZE A ESPECIFICIDADE: Escolha a categoria MAIS ESPECÍFICA possível
3. CONSIDERE O USO:
   • Doméstico vs Profissional/Industrial (ex: fogão comum → eletrodomesticos; fogão industrial → nichados)
   • Completo vs Peça (ex: carro completo → veiculos; motor avulso → partes_pecas)
   • Novo/Usado vs Sucata (ex: geladeira funcionando → eletrodomesticos; geladeira p/ reciclagem → sucatas_residuos)
4. EVITE "diversos": Use APENAS para itens abstratos ou lotes explicitamente mistos

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RESPONDA APENAS COM O NOME DA CATEGORIA (uma palavra, letras minúsculas, sem acentos).
Exemplo de resposta válida: "tecnologia" ou "veiculos" ou "imoveis"

CATEGORIA:"""
        
        return prompt
    
    def _call_groq(self, prompt: str) -> Optional[str]:
        """
        Chama API Groq com parâmetros otimizados para Llama 3.3 70B.
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um classificador especialista que analisa o CONTEXTO e a FUNÇÃO REAL dos itens. Responde apenas com o nome exato da categoria, sem explicações adicionais."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.2,      # Balanceado: consistente mas não robótico
            "max_tokens": 150,        # Suficiente para resposta + raciocínio breve
            "top_p": 0.9              # Padrão Groq otimizado
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('choices') and len(data['choices']) > 0:
                    return data['choices'][0]['message']['content'].strip()
            else:
                if self.stats['failed'] <= 3:
                    print(f"⚠️ Status {response.status_code}: {response.text[:100]}")
            
            return None
        
        except Exception as e:
            if self.stats['failed'] <= 3:
                print(f"⚠️ Erro na chamada Groq: {e}")
            return None
    
    def _extract_category(self, response: str) -> Optional[str]:
        """
        Extrai e normaliza categoria da resposta do Groq.
        Busca por nome exato de categoria ou variações comuns.
        """
        # Limpa resposta
        response_clean = response.strip().lower()
        
        # Remove pontuação comum
        for char in [',', '.', ';', ':', '\n', '"', "'", '`']:
            response_clean = response_clean.replace(char, ' ')
        
        # Pega primeira palavra (geralmente é a categoria)
        words = response_clean.split()
        if not words:
            return None
        
        first_word = words[0]
        
        # Valida categoria exata
        if first_word in self.valid_tables:
            return first_word
        
        # Mapeia variações comuns → categoria oficial
        variations = {
            # Imóveis
            'imovel': 'imoveis',
            'propriedade': 'imoveis',
            'imovel': 'imoveis',
            
            # Veículos
            'veiculo': 'veiculos',
            'veiculo': 'veiculos',
            
            # Tecnologia
            'tech': 'tecnologia',
            'tecnologias': 'tecnologia',
            
            # Eletrodomésticos
            'eletrodomestico': 'eletrodomesticos',
            'eletro': 'eletrodomesticos',
            
            # Móveis
            'movel': 'moveis_decoracao',
            'moveis': 'moveis_decoracao',
            'decoracao': 'moveis_decoracao',
            
            # Utilidades
            'utilidades': 'casa_utilidades',
            'utilidade': 'casa_utilidades',
            
            # Consumo
            'consumo': 'bens_consumo',
            'bens': 'bens_consumo',
            
            # Alimentos
            'alimento': 'alimentos_bebidas',
            'alimentos': 'alimentos_bebidas',
            'bebida': 'alimentos_bebidas',
            'bebidas': 'alimentos_bebidas',
            
            # Artes
            'arte': 'artes_colecionismo',
            'artes': 'artes_colecionismo',
            'colecionismo': 'artes_colecionismo',
            
            # Construção
            'construcao': 'materiais_construcao',
            'material': 'materiais_construcao',
            'materiais': 'materiais_construcao',
            
            # Industrial
            'industrial': 'industrial_equipamentos',
            'equipamento': 'industrial_equipamentos',
            'equipamentos': 'industrial_equipamentos',
            
            # Máquinas
            'maquina': 'maquinas_pesadas_agricolas',
            'maquinas': 'maquinas_pesadas_agricolas',
            'agricola': 'maquinas_pesadas_agricolas',
            'agricolas': 'maquinas_pesadas_agricolas',
            'pesada': 'maquinas_pesadas_agricolas',
            'pesadas': 'maquinas_pesadas_agricolas',
            
            # Nichados
            'nichado': 'nichados',
            
            # Peças
            'peca': 'partes_pecas',
            'pecas': 'partes_pecas',
            'parte': 'partes_pecas',
            'partes': 'partes_pecas',
            
            # Animais
            'animal': 'animais',
            
            # Sucatas
            'sucata': 'sucatas_residuos',
            'sucatas': 'sucatas_residuos',
            'residuo': 'sucatas_residuos',
            'residuos': 'sucatas_residuos',
        }
        
        return variations.get(first_word)
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas de classificação"""
        return self.stats.copy()
    
    def print_stats(self):
        """Imprime relatório detalhado de classificação"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS - GROQ CLASSIFIER v3.0 (100% AI)")
        print("="*80)
        print(f"Total processado:      {self.stats['total']}")
        print(f"Classificados com sucesso: {self.stats['successful']} ({self.stats['successful']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Fallback (diversos):   {self.stats['failed']} ({self.stats['failed']/max(self.stats['total'],1)*100:.1f}%)")
        
        if self.stats['by_table']:
            print(f"\n📦 DISTRIBUIÇÃO POR CATEGORIA:")
            print("-" * 80)
            
            # Ordena por quantidade
            sorted_tables = sorted(
                self.stats['by_table'].items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            for table, count in sorted_tables:
                pct = count / self.stats['total'] * 100
                bar = "█" * min(int(pct / 2), 40)
                emoji = "🎨" if table == 'diversos' else "  "
                print(f"{emoji} {table:.<30} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)
        
        # Análise de qualidade
        diversos_pct = self.stats['by_table'].get('diversos', 0) / max(self.stats['total'], 1) * 100
        success_pct = self.stats['successful'] / max(self.stats['total'], 1) * 100
        
        print(f"\n💡 ANÁLISE DE QUALIDADE:")
        print(f"   • Taxa de sucesso: {success_pct:.1f}% (ótimo se >95%)")
        print(f"   • Taxa 'diversos': {diversos_pct:.1f}% (ideal <5%)")
        
        if diversos_pct > 10:
            print(f"   ⚠️  ATENÇÃO: 'diversos' muito alto ({diversos_pct:.1f}%)")
            print(f"      → Verifique prompt e parâmetros do Groq")
        elif diversos_pct < 5:
            print(f"   ✅ Excelente! Taxa 'diversos' controlada ({diversos_pct:.1f}%)")
        
        if success_pct > 95:
            print(f"   ✅ Ótima taxa de classificação! ({success_pct:.1f}%)")
        else:
            print(f"   ⚠️  Taxa de sucesso pode melhorar ({success_pct:.1f}%)")


def classify_item_to_table(item: Dict) -> str:
    """
    Função auxiliar: classifica um único item.
    
    Args:
        item: Dict com 'normalized_title' e opcionalmente 'description'
    
    Returns:
        Nome da tabela (string)
    """
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    # TESTE DO CLASSIFICADOR
    print("\n🧪 TESTE - GROQ CLASSIFIER v3.0 (100% AI)")
    print("="*80)
    print("Zero keywords, full contextual intelligence")
    print("Llama 3.3 70B Versatile - Temperature 0.2")
    print("="*80 + "\n")
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # Imóveis
        {"normalized_title": "Apartamento 53 m² com 1 vaga - Parque das Nações", "description": "Imóvel residencial com sala, cozinha, 2 quartos"},
        {"normalized_title": "Casa 131 m² - Novo Jardim Patente - São Paulo", "description": ""},
        {"normalized_title": "Terreno urbano 300 m² - Zona Sul", "description": ""},
        
        # Veículos
        {"normalized_title": "Carro Volkswagen Gol 1.0 2015", "description": ""},
        {"normalized_title": "Moto Honda CG 150 Titan 2020", "description": ""},
        
        # Tecnologia
        {"normalized_title": "Notebook Dell Inspiron i5 8GB RAM", "description": ""},
        {"normalized_title": "iPhone 12 Pro 128GB", "description": ""},
        
        # Eletrodomésticos
        {"normalized_title": "Geladeira Brastemp Frost Free 400L", "description": ""},
        {"normalized_title": "Smart TV Samsung 55 polegadas 4K", "description": ""},
        
        # Móveis
        {"normalized_title": "Sofá 3 lugares + Poltrona estofada", "description": ""},
        
        # Nichados (profissional)
        {"normalized_title": "Fogão Industrial 6 bocas em Inox", "description": "Equipamento profissional para cozinha industrial"},
        {"normalized_title": "Cadeira Odontológica Kavo + Equipo Completo", "description": ""},
        
        # Máquinas Pesadas
        {"normalized_title": "Trator Agrícola John Deere 75HP", "description": ""},
        {"normalized_title": "Retroescavadeira Caterpillar 416F", "description": ""},
        
        # Industrial
        {"normalized_title": "Compressor de Ar Industrial 20HP", "description": ""},
        
        # Peças
        {"normalized_title": "Motor de Arranque para VW Gol (peça)", "description": ""},
        
        # Animais
        {"normalized_title": "10 cabeças de Gado Nelore", "description": ""},
        
        # Diversos (financeiros)
        {"normalized_title": "1.000 ações preferenciais Petrobras PETR4", "description": ""},
        {"normalized_title": "Marca registrada no INPI - Setor Alimentício", "description": ""},
        
        # Diversos (mistos)
        {"normalized_title": "Lote: TV 32', Geladeira, Micro-ondas, Sofá e Mesa", "description": ""},
    ]
    
    print("🔍 CLASSIFICANDO ITENS DE TESTE...\n")
    
    for i, item in enumerate(test_items, 1):
        table = classifier.classify(item)
        title_short = item['normalized_title'][:50]
        print(f"{i:>2}. {table:.<30} '{title_short}'")
    
    classifier.print_stats()