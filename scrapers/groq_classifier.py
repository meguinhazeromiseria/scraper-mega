#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER - Classificador Inteligente de Tabelas
🤖 Usa Groq AI para decidir em qual tabela cada item deve ser inserido
✨ Versão refatorada - SEM pilares, apenas tabelas diretas
"""

import json
import requests
import os
import re
from typing import Optional, Dict, List
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqTableClassifier:
    """Classificador que usa Groq para decidir a tabela correta"""
    
    # 📋 TABELAS DO BANCO - SEM PILARES!
    TABLES_INFO = {
        # ========== VAREJO E CONSUMO ==========
        'tecnologia': {
            'desc': 'Eletrônicos e informática',
            'keywords': ['notebook', 'smartphone', 'tablet', 'computador', 'monitor', 'impressora', 
                        'camera', 'drone', 'console', 'videogame', 'xbox', 'playstation', 'nintendo',
                        'smartwatch', 'fone', 'headphone', 'caixa de som', 'roteador', 'switch',
                        'mouse', 'teclado', 'webcam', 'microfone', 'ssd', 'hd externo', 'pendrive',
                        'iphone', 'ipad', 'macbook', 'samsung galaxy', 'dell', 'lenovo', 'asus', 'acer',
                        'gopro', 'dji', 'canon', 'nikon', 'sony alpha']
        },
        'eletrodomesticos': {
            'desc': 'Eletrodomésticos e linha branca',
            'keywords': ['geladeira', 'refrigerador', 'fogao', 'cooktop', 'microondas', 'lavadora',
                        'secadora', 'lava e seca', 'ar condicionado', 'ventilador', 'purificador',
                        'aspirador', 'ferro de passar', 'cafeteira', 'liquidificador', 'batedeira',
                        'processador de alimentos', 'smart tv', 'televisao', 'tv led', 'tv oled',
                        'air fryer', 'fritadeira', 'chaleira', 'torradeira', 'sanduicheira',
                        'mixer', 'espremedor', 'centrifuga', 'panela eletrica', 'grill',
                        'brastemp', 'consul', 'electrolux', 'lg', 'samsung tv', 'philips tv',
                        'panasonic', 'midea', 'britania', 'mondial', 'arno', 'black+decker']
        },
        'bens_consumo': {
            'desc': 'Bens de consumo pessoais',
            'keywords': ['roupa', 'calcado', 'sapato', 'tenis', 'bolsa', 'mochila', 'carteira',
                        'oculos', 'relogio', 'joia', 'colar', 'anel', 'brinco', 'pulseira',
                        'perfume', 'cosmetico', 'maquiagem', 'mala', 'valise', 'acessorio',
                        'bone', 'chapeu', 'cachecol', 'luva', 'cinto', 'gravata']
        },
        'veiculos': {
            'desc': 'QUALQUER meio de transporte ou locomoção',
            'keywords': ['carro', 'automovel', 'veiculo', 'moto', 'motocicleta', 'caminhao',
                        'onibus', 'van', 'pickup', 'kombi', 'trator', 'bicicleta', 'bike',
                        'patinete', 'scooter', 'patins', 'skate', 'hoverboard',
                        'jet ski', 'lancha', 'barco', 'aeronave', 'aviao', 'helicoptero',
                        # Marcas
                        'fiat', 'volkswagen', 'vw', 'ford', 'chevrolet', 'gm', 'honda', 'toyota',
                        'hyundai', 'nissan', 'renault', 'peugeot', 'citroen', 'jeep', 'mitsubishi',
                        'suzuki', 'yamaha', 'kawasaki', 'bmw', 'mercedes', 'audi', 'volvo',
                        'scania', 'iveco',
                        # Modelos comuns
                        'civic', 'corolla', 'gol', 'uno', 'palio', 'celta', 'onix', 'hb20',
                        'ka', 'fiesta', 'sandero', 'logan', 'cg 150', 'cg 160', 'fan', 'titan',
                        'factor', 'biz', 'pop', 'xre', 'bros']
        },
        'alimentos_bebidas': {
            'desc': 'Alimentos e bebidas',
            'keywords': ['alimento', 'comida', 'bebida', 'vinho', 'whisky', 'cerveja', 'cafe',
                        'cha', 'suco', 'refrigerante', 'agua', 'suplemento', 'vitamina',
                        'proteina', 'whey', 'barra de cereal', 'chocolate', 'doce']
        },
        
        # ========== CASA E DECORAÇÃO ==========
        'moveis_decoracao': {
            'desc': 'Móveis e decoração',
            'keywords': ['sofa', 'mesa', 'cadeira', 'poltrona', 'armario', 'guarda-roupa',
                        'estante', 'rack', 'cama', 'colchao', 'criado-mudo', 'comoda',
                        'aparador', 'buffet', 'cristaleira', 'escrivaninha', 'banco',
                        'pufe', 'puff', 'banqueta', 'lustres', 'luminaria', 'abajur',
                        'quadro', 'espelho', 'tapete', 'cortina', 'persiana', 'almofada',
                        'carpete', 'decoracao', 'moldura']
        },
        'casa_utilidades': {
            'desc': 'Utilidades domésticas',
            'keywords': ['panela', 'frigideira', 'assadeira', 'forma', 'louça', 'prato',
                        'tigela', 'bowl', 'talher', 'garfo', 'faca', 'colher', 'copo',
                        'xicara', 'caneca', 'jarra', 'garrafa termica', 'marmita',
                        'pote', 'organizador', 'cesto', 'vassoura', 'rodo', 'balde',
                        'escada', 'varal', 'tabua', 'kit churrasco']
        },
        'artes_colecionismo': {
            'desc': 'Arte e colecionáveis',
            'keywords': ['quadro arte', 'pintura', 'escultura', 'estatua', 'obra de arte',
                        'antiguidade', 'moeda antiga', 'selo', 'colecao', 'colecionavel',
                        'raridade', 'vintage', 'retro', 'classico', 'reliquia',
                        'porcelana antiga', 'cristal antigo']
        },
        
        # ========== IMÓVEIS E CONSTRUÇÃO ==========
        'imoveis': {
            'desc': 'Imóveis e propriedades',
            'keywords': ['imovel', 'casa', 'apartamento', 'apto', 'terreno', 'lote',
                        'galpao', 'barracao', 'sala comercial', 'loja', 'ponto comercial',
                        'fazenda', 'sitio', 'chacara', 'rural', 'urbano', 'edificio',
                        'cobertura', 'kitnet', 'studio', 'flat', 'propriedade',
                        'area', 'm2', 'm²', 'metro quadrado', 'quarto', 'suite',
                        'banheiro', 'garagem', 'vaga', 'condominio']
        },
        'materiais_construcao': {
            'desc': 'Materiais de construção',
            'keywords': ['cimento', 'tijolo', 'bloco', 'telha', 'piso', 'porcelanato',
                        'ceramica', 'azulejo', 'revestimento', 'porta', 'janela',
                        'ferragem', 'dobradiça', 'fechadura', 'tinta', 'verniz',
                        'tubo', 'cano', 'conexao', 'torneira', 'registro', 'valvula',
                        'madeira', 'tabua', 'viga', 'areia', 'brita', 'pedra',
                        'vergalhao', 'ferro', 'aco', 'colunas', 'vigas']
        },
        'industrial_equipamentos': {
            'desc': 'Equipamentos industriais',
            'keywords': ['torno', 'fresadora', 'prensa', 'compressor', 'gerador',
                        'solda', 'transformador', 'motor industrial', 'bomba industrial',
                        'valvula industrial', 'maquina cnc', 'serra industrial',
                        'furadeira industrial', 'lixadeira industrial', 'esmerilhadeira',
                        'injetora', 'extrusora', 'caldeira', 'forno industrial',
                        'equipamento de producao', 'linha de producao', 'esteira']
        },
        'maquinas_pesadas_agricolas': {
            'desc': 'Máquinas pesadas e agrícolas',
            'keywords': ['retroescavadeira', 'escavadeira', 'pa carregadeira', 'motoniveladora',
                        'rolo compactador', 'patrol', 'trator agricola', 'colheitadeira',
                        'plantadeira', 'pulverizador', 'distribuidor de adubo', 'grade',
                        'arado', 'semeadeira', 'roçadeira', 'enfardadeira', 'guincho',
                        'empilhadeira', 'bobcat', 'minicarregadeira', 'terraplenagem']
        },
        
        # ========== ESPECIALIDADES ==========
        'nichados': {
            'desc': 'Equipamentos especializados (médico, odonto, veterinário, estética, cozinha profissional)',
            'keywords': ['odontologico', 'cadeira odontologica', 'raio-x dental', 'autoclave',
                        'medico', 'hospitalar', 'clinica', 'maca', 'mesa cirurgica',
                        'bisturi', 'estetoscopio', 'equipamento medico', 'desfibrilador',
                        'veterinario', 'maquina veterinaria', 'gaiola veterinaria',
                        'estetica', 'depilacao laser', 'criolipilise', 'radiofrequencia',
                        'cozinha profissional', 'fogao industrial', 'forno industrial',
                        'coifa industrial', 'chapa industrial', 'fritadeira industrial',
                        'balcao refrigerado', 'camara fria', 'freezer industrial',
                        'laboratorio', 'centrifuga', 'microscópio', 'balanca analitica',
                        'estufa laboratorio', 'capela de exaustao']
        },
        'partes_pecas': {
            'desc': 'Peças e componentes avulsos',
            'keywords': ['peca', 'componente', 'reposicao', 'sobressalente', 'acessorio',
                        'motor (peca)', 'engrenagem', 'rolamento', 'correia', 'filtro',
                        'vela', 'bateria (peca)', 'alternador', 'radiador', 'bomba (peca)',
                        'pneu', 'aro', 'disco de freio', 'pastilha', 'amortecedor',
                        'suspensao', 'cambio (peca)', 'embreagem', 'carburador',
                        'injetor', 'sensor', 'modulo', 'central', 'chicote']
        },
        'animais': {
            'desc': 'Animais vivos',
            'keywords': ['gado', 'boi', 'vaca', 'novilho', 'bezerra', 'touro', 'cavalo',
                        'egua', 'potro', 'jumento', 'mula', 'porco', 'suino', 'galinha',
                        'frango', 'pato', 'ganso', 'peru', 'ovelha', 'carneiro', 'cabra',
                        'caprino', 'ovino', 'ave', 'passaro', 'peixe', 'alevino',
                        'cachorro', 'cao', 'gato', 'felino', 'animal vivo', 'plantel']
        },
        'sucatas_residuos': {
            'desc': 'Sucatas e materiais recicláveis',
            'keywords': ['sucata', 'residuo', 'reciclavel', 'descarte', 'ferro velho',
                        'metal sucata', 'aluminio sucata', 'cobre sucata', 'lata',
                        'papel sucata', 'papelao', 'plastico sucata', 'vidro sucata',
                        'eletronica sucata', 'bateria usada', 'aparas', 'retalho',
                        'refugo', 'resto', 'sobra', 'desmontagem']
        },
        
        # ========== DIVERSOS (RESTRITO!) ==========
        'diversos': {
            'desc': '⚠️ APENAS lotes explicitamente MISTOS com 2+ categorias diferentes no MESMO lote',
            'keywords': ['lote misto', 'itens diversos', 'produtos variados', 'mercadorias variadas',
                        'mix de produtos', 'lote variado']
        }
    }
    
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.api_url = GROQ_API_URL
        
        if not self.api_key:
            raise ValueError("⚠️ GROQ_API_KEY não encontrada! Configure no .env")
        
        self.stats = {
            'total': 0,
            'groq_classifications': 0,
            'pre_classifications': 0,
            'diversos': 0,
            'failed': 0,
            'by_table': {}
        }
    
    def _is_truly_mixed_lot(self, item: Dict) -> bool:
        """
        Verifica se é REALMENTE um lote misto (2+ categorias DIFERENTES).
        Deve ser MUITO restritivo - apenas casos óbvios.
        
        Exemplos que SÃO diversos:
        - "Lote: Cafeteira + Notebook + Mesa"
        - "Kit com itens diversos: eletrodoméstico, móvel, eletrônico"
        - "Mercadorias variadas - tecnologia e móveis"
        
        Exemplos que NÃO são diversos:
        - "Notebook Dell com mouse e teclado" (tudo tecnologia)
        - "Conjunto de panelas" (tudo casa_utilidades)
        - "Kit 3 cadeiras + mesa" (tudo móveis)
        """
        title = item.get('title', '').lower()
        desc = item.get('description', '').lower()
        text = f"{title} {desc}"
        
        # 1️⃣ PADRÕES EXPLÍCITOS de texto "diversos/misto/variado"
        explicit_patterns = [
            r'\blote\s+misto\b',
            r'\blote\s+variado\b',
            r'\bitens?\s+diversos\b',
            r'\bdiversos\s+itens?\b',
            r'\bmercadorias?\s+variadas?\b',
            r'\bprodutos?\s+variados?\b',
            r'\bmix\s+de\s+produtos?\b',
            r'\blote\s+com\s+diversos\b'
        ]
        
        has_explicit = any(re.search(p, text, re.IGNORECASE) for p in explicit_patterns)
        
        if not has_explicit:
            return False
        
        # 2️⃣ Se tem texto "diversos", verifica se REALMENTE menciona categorias diferentes
        categories_found = set()
        
        category_indicators = {
            'tecnologia': ['notebook', 'tablet', 'smartphone', 'impressora', 'monitor', 'computador'],
            'eletrodomesticos': ['geladeira', 'fogao', 'microondas', 'tv', 'televisao', 'lavadora'],
            'moveis': ['sofa', 'mesa', 'cadeira', 'armario', 'cama', 'estante'],
            'casa_utilidades': ['panela', 'prato', 'copo', 'talher', 'louça'],
            'veiculos': ['carro', 'moto', 'caminhao', 'bicicleta'],
            'imoveis': ['casa', 'apartamento', 'terreno', 'imovel']
        }
        
        for category, indicators in category_indicators.items():
            if any(indicator in text for indicator in indicators):
                categories_found.add(category)
        
        # Se menciona 2+ categorias diferentes, é diversos
        return len(categories_found) >= 2
    
    def _pre_classify_obvious(self, item: Dict) -> Optional[str]:
        """
        Pré-classifica itens óbvios SEM usar Groq.
        Economiza chamadas de API e melhora velocidade.
        """
        title = item.get('title', '').lower()
        desc = item.get('description', '').lower()
        text = f"{title} {desc}"
        
        # Remove metadata que pode confundir (ex: "categoria: veiculos")
        text = re.sub(r'categoria\s*:\s*\w+', '', text)
        text = re.sub(r'secao\s*:\s*\w+', '', text)
        
        # Conta matches por tabela
        matches_by_table = {}
        
        for table, info in self.TABLES_INFO.items():
            if table == 'diversos':  # Pula diversos na pré-classificação
                continue
            
            keywords = info.get('keywords', [])
            matches = sum(1 for kw in keywords if kw in text)
            
            if matches > 0:
                matches_by_table[table] = matches
        
        # Se nenhum match, retorna None (vai pro Groq)
        if not matches_by_table:
            return None
        
        # Retorna tabela com mais matches
        best_table = max(matches_by_table.items(), key=lambda x: x[1])
        
        # Só retorna se tiver pelo menos 2 matches (mais confiante)
        if best_table[1] >= 2:
            return best_table[0]
        
        # Se só 1 match mas muito óbvio (ex: marca de carro), aceita
        obvious_single_match_tables = ['veiculos', 'imoveis']
        if best_table[1] == 1 and best_table[0] in obvious_single_match_tables:
            # Verifica se é match forte
            table_keywords = self.TABLES_INFO[best_table[0]]['keywords']
            strong_keywords = {
                'veiculos': ['fiat', 'ford', 'honda', 'toyota', 'yamaha', 'civic', 'corolla'],
                'imoveis': ['apartamento', 'terreno', 'casa', 'lote', 'imovel']
            }
            
            if best_table[0] in strong_keywords:
                if any(kw in text for kw in strong_keywords[best_table[0]]):
                    return best_table[0]
        
        return None
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica um item e retorna o nome da tabela.
        
        Fluxo:
        1. Verifica se é lote misto EXPLÍCITO → diversos
        2. Tenta pré-classificação com keywords → tabela específica
        3. Usa Groq AI → tabela específica
        4. Fallback → diversos
        """
        title = item.get('title', '').strip()
        description = item.get('description', '')[:500]
        
        if not title:
            self.stats['failed'] += 1
            self.stats['total'] += 1
            return None
        
        # 1️⃣ VERIFICA SE É LOTE MISTO EXPLÍCITO
        if self._is_truly_mixed_lot(item):
            self.stats['diversos'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            # Debug primeiros
            if self.stats['diversos'] <= 5:
                print(f"  🎨 DIVERSOS (misto real): '{title[:70]}'")
            
            return 'diversos'
        
        # 2️⃣ PRÉ-CLASSIFICAÇÃO COM KEYWORDS
        pre_classified = self._pre_classify_obvious(item)
        
        if pre_classified:
            self.stats['pre_classifications'] += 1
            self.stats['by_table'][pre_classified] = self.stats['by_table'].get(pre_classified, 0) + 1
            self.stats['total'] += 1
            
            # Debug primeiros de cada categoria
            table_count = self.stats['by_table'][pre_classified]
            if table_count <= 3:
                print(f"  ⚡ PRÉ-CLASS {pre_classified}: '{title[:60]}'")
            
            return pre_classified
        
        # 3️⃣ CLASSIFICAÇÃO COM GROQ
        table_name = self._classify_with_groq(title, description)
        
        if table_name and table_name != 'diversos':
            self.stats['groq_classifications'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            
            # Debug primeiros
            if self.stats['groq_classifications'] <= 10:
                print(f"  🤖 GROQ {table_name}: '{title[:60]}'")
            
            return table_name
        
        # 4️⃣ FALLBACK: DIVERSOS (último recurso)
        self.stats['diversos'] += 1
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """Classifica com Groq e retorna a tabela"""
        prompt = self._build_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if response:
                response_clean = response.strip().lower()
                
                # Remove explicações extras
                if '\n' in response_clean:
                    response_clean = response_clean.split('\n')[0]
                
                response_clean = response_clean.replace(',', '').replace(';', '').strip()
                
                # Valida se é tabela válida
                if response_clean in self.TABLES_INFO:
                    return response_clean
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro Groq: {e}")
            return None
    
    def _build_prompt(self, title: str, description: str) -> str:
        """Monta prompt direto para Groq"""
        
        # Lista simples de tabelas
        tables_list = "\n".join([
            f"- {table}: {info['desc']}"
            for table, info in self.TABLES_INFO.items()
        ])
        
        prompt = f"""Você é um classificador de leilões brasileiro. Identifique a categoria MAIS ESPECÍFICA.

CATEGORIAS:
{tables_list}

ITEM:
Título: {title}
Descrição: {description[:300] if description else 'N/A'}

REGRAS CRÍTICAS:

🏠 IMÓVEIS (máxima prioridade):
- Casa, apartamento, terreno, lote, galpão → "imoveis"
- Se mencionar m², quartos, suítes → "imoveis"

🚗 VEÍCULOS:
- Carro, moto, caminhão, ônibus, bicicleta → "veiculos"
- Se mencionar marca (Fiat, Honda, etc) → "veiculos"

💻 TECNOLOGIA vs 📺 ELETRODOMÉSTICOS:
- Notebook, smartphone, impressora → "tecnologia"
- Smart TV, geladeira, fogão, air fryer → "eletrodomesticos"

🔧 NICHADOS:
- Odontológico, hospitalar, cozinha industrial → "nichados"

⚠️ DIVERSOS:
- APENAS se explicitamente "lote misto" com categorias diferentes
- Se tem categoria clara, NÃO use diversos

RESPOSTA (apenas o nome da categoria):"""
        
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
                    "content": "Você é um classificador preciso. Responda APENAS com o nome da categoria. Uma palavra."
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
        """Imprime estatísticas detalhadas"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS DE CLASSIFICAÇÃO GROQ")
        print("="*80)
        print(f"Total processado: {self.stats['total']}")
        print(f"Pré-classificações (keywords): {self.stats['pre_classifications']} ({self.stats['pre_classifications']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Classificações Groq: {self.stats['groq_classifications']} ({self.stats['groq_classifications']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Diversos (lotes mistos): {self.stats['diversos']} ({self.stats['diversos']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Falhas: {self.stats['failed']}")
        
        if self.stats['by_table']:
            print(f"\n📦 DISTRIBUIÇÃO POR TABELA:")
            print("-" * 80)
            
            for table, count in sorted(self.stats['by_table'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['total'] * 100
                bar_length = int(pct / 2)
                bar = "█" * bar_length
                
                emoji = "🎨" if table == 'diversos' else "  "
                print(f"{emoji} {table:.<35} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)


# Função auxiliar
def classify_item_to_table(item: Dict) -> str:
    """Classifica um item e retorna a tabela"""
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    print("\n🤖 TESTANDO CLASSIFICADOR - VERSÃO SEM PILARES\n")
    print("="*80)
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # IMÓVEIS (devem ir para imoveis, não diversos!)
        {"title": "Apartamento 2 Quartos - 65m²", "description": "Apto com 2 quartos, sala, cozinha"},
        {"title": "Casa 3 Dormitórios Centro", "description": "Casa de 120m² com garagem"},
        {"title": "Terreno 250m² Residencial", "description": "Lote em condomínio fechado"},
        
        # VEÍCULOS
        {"title": "Fiat Uno 2015 Completo", "description": "Carro 4 portas"},
        {"title": "Honda CG 160 2020", "description": "Moto em bom estado"},
        {"title": "Bicicleta Caloi Aro 29", "description": "Mountain bike 21 marchas"},
        
        # TECNOLOGIA
        {"title": "Notebook Dell Inspiron i5 8GB", "description": "Notebook completo"},
        {"title": "iPhone 13 Pro 256GB", "description": "Smartphone Apple"},
        {"title": "Impressora HP LaserJet", "description": "Multifuncional"},
        
        # ELETRODOMÉSTICOS
        {"title": "Smart TV Samsung 55\" 4K", "description": "Televisão inteligente"},
        {"title": "Geladeira Brastemp Inverse", "description": "Frost free 400L"},
        {"title": "Air Fryer Philips Walita", "description": "Fritadeira 4L"},
        
        # MÓVEIS
        {"title": "Sofá 3 Lugares Retrátil", "description": "Sofá tecido cinza"},
        {"title": "Mesa Jantar 6 Cadeiras", "description": "Conjunto completo"},
        
        # DIVERSOS (VERDADEIROS - lotes mistos)
        {"title": "Lote Misto: Geladeira + Notebook + Mesa", "description": "Produtos variados"},
        {"title": "Kit Diversos: TV + Bicicleta + Panelas", "description": "Lote com categorias diferentes"},
        
        # NÃO DEVEM ser diversos (mesmo tendo múltiplos itens da MESMA categoria)
        {"title": "Kit 3 Cadeiras + Mesa Jantar", "description": "Conjunto de móveis"},
        {"title": "Lote 10 Notebooks Dell e HP", "description": "Notebooks diversos modelos"},
    ]
    
    print("🔍 CLASSIFICANDO ITENS DE TESTE...\n")
    
    for i, item in enumerate(test_items, 1):
        table = classifier.classify(item)
        print(f"{i:02d}. '{item['title'][:65]}'")
        print(f"    └─ 📂 Tabela: {table}")
        print()
    
    classifier.print_stats()
    print("\n✅ Teste concluído!")
    print("="*80)