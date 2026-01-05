#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER - Classificador Inteligente de Tabelas
🤖 Usa Groq AI para decidir em qual tabela cada item deve ser inserido
✨ Versão refatorada - Oportunidades agora é apenas uma VIEW SQL
"""

import json
import requests
import os
import re
from typing import Optional, Dict, List
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# Chave API Groq - agora lida do .env
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqTableClassifier:
    """Classificador que usa Groq para decidir a tabela correta"""
    
    # 📋 TABELAS DO BANCO DE DADOS
    # Organizado por Pilares para melhor organização
    
    TABLES_INFO = {
        # ==================== PILAR 1: VAREJO E CONSUMO DIRETO ====================
        'bens_consumo': {
            'desc': 'Bens de consumo diversos e artigos pessoais',
            'exemplos': 'roupas, calçados, bolsas, acessórios, cosméticos, perfumes, produtos de higiene, joias, relógios, malas',
            'pilar': 1
        },
        'eletrodomesticos': {
            'desc': 'Eletrodomésticos e linha branca para uso residencial',
            'exemplos': 'geladeiras, fogões, micro-ondas, lavadoras, secadoras, ar condicionado, ventiladores, purificadores, aspiradores, ferros de passar, cafeteiras, liquidificadores, batedeiras, smart TVs, air fryers',
            'pilar': 1
        },
        'tecnologia': {
            'desc': 'Produtos eletrônicos e de informática',
            'exemplos': 'notebooks, smartphones, tablets, computadores, monitores, impressoras, câmeras, drones, consoles de videogame, smartwatches, fones, caixas de som, roteadores, switches, periféricos, componentes de PC',
            'pilar': 1
        },
        'veiculos': {
            'desc': 'QUALQUER meio de transporte ou locomoção, motorizado ou não',
            'exemplos': 'carros, motos, caminhões, ônibus, tratores, bicicletas, patins, patinetes, skates, scooters, hoverboards, veículos elétricos, jet ski, lanchas, barcos, aeronaves, qualquer coisa usada para se locomover',
            'pilar': 1
        },
        'alimentos_bebidas': {
            'desc': 'Alimentos e bebidas',
            'exemplos': 'alimentos não perecíveis, bebidas, vinhos, cafés, suplementos alimentares, produtos alimentícios',
            'pilar': 1
        },
        
        # ==================== PILAR 2: CASA E DECORAÇÃO ====================
        'moveis_decoracao': {
            'desc': 'Móveis e itens de decoração',
            'exemplos': 'sofás, mesas, cadeiras, armários, estantes, camas, colchões, lustres, quadros, tapetes, cortinas, pufes, racks, cristaleiras',
            'pilar': 2
        },
        'casa_utilidades': {
            'desc': 'Utilidades domésticas e itens de casa pequenos',
            'exemplos': 'panelas, louças, talheres, copos, utensílios de cozinha, organizadores, produtos de limpeza, pequenos objetos domésticos',
            'pilar': 2
        },
        'artes_colecionismo': {
            'desc': 'Arte, antiguidades e colecionáveis',
            'exemplos': 'quadros, esculturas, antiguidades, moedas, selos, itens colecionáveis, obras de arte, objetos raros',
            'pilar': 2
        },
        
        # ==================== PILAR 3: IMÓVEIS E CONSTRUÇÃO ====================
        'imoveis': {
            'desc': 'Imóveis e propriedades',
            'exemplos': 'casas, apartamentos, terrenos, galpões, salas comerciais, fazendas, chácaras, sítios, lotes, propriedades rurais',
            'pilar': 3
        },
        'materiais_construcao': {
            'desc': 'Materiais de construção e acabamento',
            'exemplos': 'cimento, tijolos, telhas, pisos, azulejos, portas, janelas, ferragens, tintas, tubos, madeiras, areia, brita, vergalhões',
            'pilar': 3
        },
        'industrial_equipamentos': {
            'desc': 'Equipamentos e máquinas industriais para manufatura',
            'exemplos': 'tornos, fresadoras, prensas, compressores, geradores, soldas, equipamentos de fábrica, máquinas CNC, injetoras, extrusoras, equipamentos de produção',
            'pilar': 3
        },
        'maquinas_pesadas_agricolas': {
            'desc': 'Máquinas pesadas e equipamentos agrícolas',
            'exemplos': 'retroescavadeiras, escavadeiras, tratores agrícolas, colheitadeiras, plantadeiras, pulverizadores, pás carregadeiras, motoniveladoras, rolos compactadores',
            'pilar': 3
        },
        
        # ==================== PILAR 4: ESPECIALIDADES E DIVERSOS ====================
        'nichados': {
            'desc': 'Equipamentos e produtos especializados de setores específicos: saúde, odontologia, veterinária, cozinha profissional, laboratórios, estética',
            'exemplos': 'máquina de raio-x odontológico, cadeira odontológica, autoclave, equipamentos médicos, armários hospitalares, mesas cirúrgicas, coifas industriais, fogões industriais, fornos profissionais, equipamentos de laboratório, centrífugas, equipamentos de estética, câmaras frias',
            'pilar': 4
        },
        'partes_pecas': {
            'desc': 'Peças, componentes e acessórios avulsos',
            'exemplos': 'peças automotivas, peças de máquinas, componentes eletrônicos, peças de reposição, sobressalentes, acessórios, partes de equipamentos',
            'pilar': 4
        },
        'animais': {
            'desc': 'Animais vivos',
            'exemplos': 'gado, cavalos, aves, animais de estimação, animais de produção, animais de criação',
            'pilar': 4
        },
        'sucatas_residuos': {
            'desc': 'Sucatas, resíduos e materiais para reciclagem',
            'exemplos': 'sucata de metal, materiais recicláveis, resíduos industriais, lotes de descarte, ferro velho, sucata eletrônica',
            'pilar': 4
        },
        'diversos': {
            'desc': '🎯 LOTES MISTOS E ITENS DIVERSOS - Para itens que explicitamente combinam múltiplas categorias diferentes OU descritos como "diversos"',
            'exemplos': 'APENAS itens com texto literal tipo "itens diversos", "lote misto", "mercadorias variadas" OU combinações explícitas tipo "Kit Notebook + Impressora", "Lote: Cafeteira + Tablet + Fones"',
            'pilar': 4,
            'special': True
        }
    }
    
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.api_url = GROQ_API_URL
        
        # Valida se a chave API foi configurada
        if not self.api_key:
            raise ValueError(
                "⚠️ GROQ_API_KEY não encontrada! "
                "Configure a variável de ambiente no arquivo .env"
            )
        
        # Estatísticas
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'diversos': 0,
            'pre_classified_vehicles': 0,
            'by_table': {}
        }
    
    def _is_explicit_diversos(self, item: Dict) -> bool:
        """
        Verifica se o item deve ir para 'diversos' SEM usar Groq
        Apenas para casos EXPLÍCITOS de lotes mistos
        
        Returns:
            bool: True se for diversos explícito
        """
        title = item.get('title', '').lower()
        description = item.get('description', '').lower()
        text = f"{title} {description}"
        
        # Padrões EXPLÍCITOS de lotes diversos/mistos (bem específicos!)
        diversos_patterns = [
            r'\bitens?\s+diversos\b',
            r'\bdiversos\s+itens?\b',
            r'\blote\s+misto\b',
            r'\blote\s+variado\b',
            r'\bmercadorias?\s+variadas?\b',
            r'\bprodutos?\s+variados?\b',
            r'\bmix\s+de\b',
            r'\blote\s+com\s+diversos\b',
            r'\bvarios\s+itens?\b',
            r'\blote\s+de\s+produtos?\s+variados?\b',
        ]
        
        for pattern in diversos_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        
        # Detecta combinações explícitas tipo "Notebook + Impressora"
        # Procura por múltiplos itens separados por + ou ,
        plus_pattern = r'(\w+)\s*\+\s*(\w+)'
        if re.search(plus_pattern, title, re.IGNORECASE):
            # Verifica se tem pelo menos 2 categorias diferentes mencionadas
            categories_mentioned = []
            title_lower = title.lower()
            
            # Palavras-chave de diferentes categorias
            category_keywords = {
                'tecnologia': ['notebook', 'tablet', 'smartphone', 'celular', 'computador', 'monitor'],
                'eletrodomesticos': ['geladeira', 'fogao', 'lavadora', 'microondas', 'cafeteira'],
                'moveis': ['mesa', 'cadeira', 'sofa', 'armario'],
                'casa': ['panela', 'louça', 'copo', 'prato']
            }
            
            for cat, keywords in category_keywords.items():
                for keyword in keywords:
                    if keyword in title_lower:
                        categories_mentioned.append(cat)
                        break
            
            # Se menciona 2+ categorias diferentes, é diversos
            if len(set(categories_mentioned)) >= 2:
                return True
        
        return False
    
    def _is_obvious_vehicle(self, item: Dict) -> bool:
        """
        Detecta veículos óbvios sem precisar chamar Groq
        
        Returns:
            bool: True se for veículo óbvio
        """
        title = item.get('title', '').lower()
        
        # Marcas de veículos
        vehicle_brands = [
            'fiat', 'volkswagen', 'vw', 'ford', 'chevrolet', 'gm',
            'honda', 'toyota', 'hyundai', 'nissan', 'renault',
            'peugeot', 'citroen', 'jeep', 'mitsubishi', 'suzuki',
            'yamaha', 'kawasaki', 'bmw', 'mercedes', 'audi',
            'volvo', 'scania', 'iveco', 'mercedes-benz'
        ]
        
        # Modelos comuns
        vehicle_models = [
            'civic', 'corolla', 'gol', 'uno', 'palio', 'celta',
            'onix', 'hb20', 'ka', 'fiesta', 'sandero', 'logan',
            'cg 150', 'cg 160', 'fan', 'titan', 'factor', 'biz'
        ]
        
        # Palavras-chave de veículos
        vehicle_keywords = [
            'carro', 'moto', 'caminhao', 'caminhão', 'onibus', 'ônibus',
            'motocicleta', 'automovel', 'automóvel', 'veiculo', 'veículo',
            'pickup', 'van', 'kombi', 'bicicleta', 'bike', 'patinete',
            'scooter', 'jet ski', 'lancha', 'barco', 'aeronave'
        ]
        
        # Verifica marcas
        for brand in vehicle_brands:
            if brand in title:
                return True
        
        # Verifica modelos
        for model in vehicle_models:
            if model in title:
                return True
        
        # Verifica palavras-chave
        for keyword in vehicle_keywords:
            if keyword in title:
                return True
        
        # Padrão de ano (ex: "2020", "2015/2016")
        if re.search(r'\b(19|20)\d{2}\b', title):
            # Se tem ano, verifica se não é imóvel
            if not any(word in title for word in ['apartamento', 'casa', 'terreno', 'sala', 'lote']):
                return True
        
        return False
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica um item e retorna o nome da tabela
        
        Args:
            item: Dict com 'title' e opcionalmente 'description'
        
        Returns:
            Nome da tabela (ex: 'tecnologia', 'veiculos') ou None se falhar
        """
        title = item.get('title', '').strip()
        description = item.get('description', '')[:500]
        
        if not title:
            self.stats['failed'] += 1
            self.stats['total'] += 1
            return None
        
        # PRÉ-VERIFICAÇÃO 1: Verifica se é "diversos" explícito
        if self._is_explicit_diversos(item):
            self.stats['diversos'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            # Debug primeiros diversos
            if self.stats['diversos'] <= 10:
                print(f"  🎨 Diversos detectado: '{title[:70]}'")
            return 'diversos'
        
        # PRÉ-VERIFICAÇÃO 2: Verifica se é veículo óbvio
        if self._is_obvious_vehicle(item):
            self.stats['pre_classified_vehicles'] += 1
            self.stats['by_table']['veiculos'] = self.stats['by_table'].get('veiculos', 0) + 1
            self.stats['total'] += 1
            # Debug primeiros veículos
            if self.stats['pre_classified_vehicles'] <= 10:
                print(f"  ✅ Veículo detectado: '{title[:70]}'")
            return 'veiculos'
        
        # Classifica com Groq
        table_name = self._classify_with_groq(title, description)
        
        if table_name:
            self.stats['success'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            return table_name
        
        # Fallback para diversos
        self.stats['diversos'] += 1
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """
        Classifica com Groq e retorna a tabela
        
        Returns:
            str: nome da tabela ou None se falhar
        """
        prompt = self._build_classification_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if response:
                # Parse da resposta - deve ser apenas uma categoria
                response_clean = response.strip().lower()
                
                # Remove possíveis explicações extras
                if '\n' in response_clean:
                    response_clean = response_clean.split('\n')[0]
                
                # Remove espaços e possíveis vírgulas/separadores
                response_clean = response_clean.replace(',', '').replace(';', '').strip()
                
                # Valida se é uma tabela válida
                if response_clean in self.TABLES_INFO:
                    return response_clean
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro na classificação: {e}")
            return None
    
    def _build_classification_prompt(self, title: str, description: str) -> str:
        """Monta o prompt para o Groq"""
        
        # Lista de tabelas (incluindo diversos, mas com sua descrição especial)
        tables_list = []
        for table, info in self.TABLES_INFO.items():
            tables_list.append(f"- {table}: {info['desc']}")
            tables_list.append(f"  Exemplos: {info['exemplos']}")
        
        tables_text = "\n".join(tables_list)
        
        prompt = f"""Você é um classificador de leilões brasileiro. Analise o item e identifique a categoria MAIS ESPECÍFICA.

CATEGORIAS DISPONÍVEIS:
{tables_text}

ITEM PARA CLASSIFICAR:
Título: {title}
Descrição: {description[:300] if description else 'Não disponível'}

REGRAS CRÍTICAS - LEIA COM ATENÇÃO:

🚗 VEÍCULOS (PRIORIDADE MÁXIMA):
- Carros, motos, caminhões, ônibus → SEMPRE "veiculos"
- Bicicletas, patins, patinetes, skates → SEMPRE "veiculos"
- Se mencionar marca (Fiat, VW, Honda, Yamaha, Ford, etc) → "veiculos"
- Se mencionar modelo (Civic, Gol, Corolla, CG, etc) → "veiculos"
- Se mencionar ano do veículo → "veiculos"
- Qualquer coisa que serve para locomoção → "veiculos"

🏠 IMÓVEIS:
- Casas, apartamentos, terrenos, lotes → "imoveis"
- Nunca confunda imóveis com veículos

🔧 NICHADOS (equipamentos especializados):
- Odontológico, hospitalar, laboratório → "nichados"
- Cozinha industrial, fogão industrial → "nichados"

📺 ELETRODOMÉSTICOS vs TECNOLOGIA:
- Smart TV, geladeira, fogão, lavadora → "eletrodomesticos"
- Air Fryer, cafeteira, micro-ondas → "eletrodomesticos"
- Notebook, tablet, smartphone, impressora → "tecnologia"

🎯 DIVERSOS:
- SOMENTE se título diz "lote misto" ou "itens diversos"
- Se tem categoria clara, NÃO use diversos

IMPORTANTE: 
- Responda APENAS o nome da categoria
- Uma palavra, sem explicação
- Em caso de dúvida entre 2 categorias, escolha a MAIS ESPECÍFICA

RESPOSTA (apenas o nome da categoria):"""
        
        return prompt
    
    def _call_groq(self, prompt: str) -> Optional[str]:
        """Chama a API Groq"""
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {
                    "role": "system",
                    "content": "Você é um classificador preciso de leilões. Responda APENAS com o nome da categoria. Uma palavra. Sem explicações."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0,
            "max_tokens": 50,
            "top_p": 0.9
        }
        
        try:
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('choices') and len(data['choices']) > 0:
                    return data['choices'][0]['message']['content'].strip()
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro na chamada Groq: {e}")
            return None
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas de classificação"""
        return self.stats.copy()
    
    def print_stats(self):
        """Imprime estatísticas detalhadas"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS DE CLASSIFICAÇÃO GROQ")
        print("="*80)
        print(f"Total processado: {self.stats['total']}")
        print(f"Sucesso (via Groq): {self.stats['success']} ({self.stats['success']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Veículos (pré-classificação): {self.stats['pre_classified_vehicles']} ({self.stats['pre_classified_vehicles']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Diversos (pré-classificação): {self.stats['diversos']} ({self.stats['diversos']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Falhas: {self.stats['failed']}")
        
        if self.stats['by_table']:
            # Organiza por pilar
            by_pillar = {1: {}, 2: {}, 3: {}, 4: {}}
            for table, count in self.stats['by_table'].items():
                pilar = self.TABLES_INFO.get(table, {}).get('pilar', 4)
                by_pillar[pilar][table] = count
            
            pilar_names = {
                1: "🛒 PILAR 1: Varejo e Consumo Direto",
                2: "🏠 PILAR 2: Casa e Decoração",
                3: "🏗️  PILAR 3: Imóveis e Construção",
                4: "🎯 PILAR 4: Especialidades e Diversos"
            }
            
            print(f"\n📦 DISTRIBUIÇÃO POR PILAR E TABELA:")
            print("-" * 80)
            
            for pilar_num in [1, 2, 3, 4]:
                if by_pillar[pilar_num]:
                    pilar_total = sum(by_pillar[pilar_num].values())
                    pilar_pct = pilar_total / self.stats['total'] * 100
                    print(f"\n{pilar_names[pilar_num]}")
                    print(f"Total: {pilar_total} itens ({pilar_pct:.1f}%)")
                    print("-" * 80)
                    
                    for table, count in sorted(by_pillar[pilar_num].items(), key=lambda x: x[1], reverse=True):
                        pct = count / self.stats['total'] * 100
                        bar_length = int(pct / 2)  # Escala a barra
                        bar = "█" * bar_length
                        
                        # Emoji especial para diversos
                        emoji = "🎨" if table == 'diversos' else "  "
                        
                        print(f"{emoji} {table:.<35} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)


# Função auxiliar para uso fácil
def classify_item_to_table(item: Dict) -> str:
    """
    Classifica um item e retorna a tabela
    
    Args:
        item: Dict com 'title' e opcionalmente 'description'
    
    Returns:
        Nome da tabela (string)
    """
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    print("\n🤖 TESTANDO CLASSIFICADOR GROQ - VERSÃO REFATORADA\n")
    print("="*80)
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # ==================== DIVERSOS (EXPLÍCITOS) ====================
        {
            "title": "Lote com Itens Diversos",
            "description": "Vários produtos de diferentes categorias"
        },
        {
            "title": "Lote Misto de Mercadorias",
            "description": "Produtos variados"
        },
        {
            "title": "Kit: Notebook Dell + Impressora HP + Mouse Logitech",
            "description": "Combo de equipamentos de informática"
        },
        {
            "title": "Cafeteira Philips + Tablet Samsung + Fones JBL",
            "description": "Lote combinado"
        },
        
        # ==================== ELETRODOMÉSTICOS ====================
        {
            "title": "Smart TV Samsung 55 Polegadas 4K",
            "description": "Televisão inteligente com sistema operacional"
        },
        {
            "title": "Air Fryer Philips Walita com Conectividade",
            "description": "Fritadeira elétrica com app"
        },
        {
            "title": "Geladeira Brastemp Inverse",
            "description": "Geladeira frost free"
        },
        {
            "title": "Micro-ondas Electrolux 30L",
            "description": "Micro-ondas com grill"
        },
        {
            "title": "Cafeteira Nespresso Inissia",
            "description": "Máquina de café expresso"
        },
        
        # ==================== TECNOLOGIA ====================
        {
            "title": "Notebook Dell Inspiron 15",
            "description": "Notebook com Intel Core i5 e 8GB RAM"
        },
        {
            "title": "iPhone 13 Pro Max 256GB",
            "description": "Smartphone Apple"
        },
        {
            "title": "iPad 9ª Geração",
            "description": "Tablet Apple com 64GB"
        },
        {
            "title": "Impressora HP LaserJet Pro",
            "description": "Impressora multifuncional"
        },
        
        # ==================== VEÍCULOS ====================
        {
            "title": "Bicicleta Caloi Mountain Bike Aro 29",
            "description": "Bicicleta 21 marchas"
        },
        {
            "title": "Patinete Elétrico Xiaomi",
            "description": "Patinete com autonomia de 30km"
        },
        {
            "title": "Civic 2020 Automático",
            "description": "Honda Civic completo"
        },
        
        # ==================== MÓVEIS ====================
        {
            "title": "Sofá 3 Lugares Retrátil",
            "description": "Sofá em tecido cinza"
        },
        {
            "title": "Mesa de Jantar 6 Cadeiras",
            "description": "Conjunto completo"
        },
        
        # ==================== NICHADOS ====================
        {
            "title": "Cadeira Odontológica Kavo",
            "description": "Equipamento odontológico completo"
        },
        {
            "title": "Autoclave Cristofoli 21L",
            "description": "Autoclave para esterilização"
        },
        {
            "title": "Fogão Industrial 6 Bocas",
            "description": "Fogão profissional para cozinha comercial"
        },
    ]
    
    print("\n🔍 CLASSIFICANDO ITENS DE TESTE...\n")
    
    for i, item in enumerate(test_items, 1):
        table = classifier.classify(item)
        
        print(f"{i:02d}. '{item['title'][:65]}'")
        print(f"    └─ 📂 Tabela: {table}")
        print()
    
    # Imprime estatísticas
    classifier.print_stats()
    
    print("\n✅ Teste concluído!")
    print("="*80)