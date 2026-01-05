#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER - Classificador Inteligente de Tabelas
🤖 Usa Groq AI para decidir em qual tabela cada item deve ser inserido
✨ Agora com suporte para múltiplas categorias (tabela diversos)
"""

import json
import requests
import os
import re
from typing import Optional, Dict, List, Tuple
from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

# Chave API Groq - agora lida do .env
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqTableClassifier:
    """Classificador que usa Groq para decidir a tabela correta"""
    
    # 📋 PILARES E CATEGORIAS
    # Pilar 1: Varejo e Consumo Direto
    # Pilar 2: Casa e Decoração
    # Pilar 3: Imóveis e Construção
    # Pilar 4: Especialidades e Diversos
    
    TABLES_INFO = {
        # ==================== PILAR 1: VAREJO E CONSUMO DIRETO ====================
        'bens_consumo': {
            'desc': 'Bens de consumo diversos e artigos pessoais',
            'exemplos': 'roupas, calçados, bolsas, acessórios, cosméticos, perfumes, produtos de higiene, joias, relógios',
            'pilar': 1
        },
        'eletrodomesticos': {
            'desc': 'Eletrodomésticos e linha branca para uso residencial',
            'exemplos': 'geladeiras, fogões, micro-ondas, lavadoras, secadoras, ar condicionado, ventiladores, purificadores, aspiradores, ferros de passar, cafeteiras, liquidificadores, batedeiras',
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
        'alimentos_bebidas': {
            'desc': 'Alimentos e bebidas',
            'exemplos': 'alimentos não perecíveis, bebidas, vinhos, cafés, suplementos alimentares, produtos alimentícios',
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
            'desc': '🎯 MÚLTIPLAS CATEGORIAS - Itens que pertencem a 2 ou mais categorias simultaneamente',
            'exemplos': 'Smart TV (tecnologia + eletrodomesticos), Air Fryer com Wi-Fi (tecnologia + eletrodomesticos), Geladeira Inteligente (tecnologia + eletrodomesticos), Smartwatch fitness (tecnologia + bens_consumo)',
            'pilar': 4,
            'special': True  # Marca como tabela especial
        },
        'oportunidades': {
            'desc': 'Oportunidades gerais, lotes mistos e itens não classificáveis nas outras categorias',
            'exemplos': 'lotes mistos, itens variados, oportunidades gerais sem categoria específica, mercadorias diversas',
            'pilar': 4
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
            'auto_oportunidades': 0,
            'diversos': 0,  # Contador de itens com múltiplas categorias
            'by_table': {},
            'oportunidades_reasons': {},
            'diversos_combinations': {}  # Combinações de categorias em diversos
        }
    
    def _is_opportunity_item(self, item: Dict) -> tuple[bool, str]:
        """
        Verifica se o item deve ir direto para 'oportunidades'
        
        Returns:
            (bool, str): (é_oportunidade, motivo)
        """
        title = item.get('title', '').lower()
        description = item.get('description', '').lower()
        text = f"{title} {description}"
        
        # 1. ITENS COM LANCES (já tem competição)
        total_bids = item.get('total_bids', 0) or 0
        if total_bids > 0:
            return True, f'tem_lances ({total_bids})'
        
        # 2. MUITOS COMPRADORES/LICITANTES
        total_bidders = item.get('total_bidders', 0) or 0
        if total_bidders >= 3:
            return True, f'muitos_compradores ({total_bidders})'
        
        # 3. MUITAS UNIDADES (lotes com múltiplas unidades)
        quantity_patterns = [
            r'(\d+)\s*(?:unidades|unids?|peças|pçs|itens|produtos)',
            r'lote\s+(?:com|de)\s+(\d+)',
            r'quantidade[:\s]+(\d+)',
        ]
        
        for pattern in quantity_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                try:
                    qty = int(match.group(1))
                    if qty >= 10:
                        return True, f'muitas_unidades ({qty})'
                except:
                    pass
        
        # 4. SEGUNDA PRAÇA
        segunda_praca_keywords = [
            'segunda praça',
            '2ª praça',
            '2a praça',
            'segunda praca',
            'novo pregão',
            'nova tentativa',
        ]
        
        for keyword in segunda_praca_keywords:
            if keyword in text:
                return True, 'segunda_praca'
        
        # 5. LOTES MISTOS (múltiplos itens diferentes)
        lote_misto_keywords = [
            'lote misto',
            'lote variado',
            'itens diversos',
            'diversos itens',
            'mercadorias variadas',
            'produtos variados',
            'sortidos',
            'mix de',
        ]
        
        for keyword in lote_misto_keywords:
            if keyword in text:
                return True, 'lote_misto'
        
        return False, ''
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        Classifica um item e retorna o nome da tabela
        
        Args:
            item: Dict com 'title' e opcionalmente 'description'
        
        Returns:
            Nome da tabela (ex: 'tecnologia', 'veiculos', 'diversos') ou None se falhar
        """
        title = item.get('title', '').strip()
        description = item.get('description', '')[:500]
        
        if not title:
            return None
        
        # PRÉ-CLASSIFICAÇÃO: Verifica se é oportunidade automaticamente
        is_opportunity, reason = self._is_opportunity_item(item)
        
        if is_opportunity:
            self.stats['auto_oportunidades'] += 1
            self.stats['by_table']['oportunidades'] = self.stats['by_table'].get('oportunidades', 0) + 1
            self.stats['oportunidades_reasons'][reason] = self.stats['oportunidades_reasons'].get(reason, 0) + 1
            self.stats['total'] += 1
            return 'oportunidades'
        
        # Classifica com Groq (agora pode retornar múltiplas categorias)
        result = self._classify_with_groq(title, description)
        
        if result:
            table_name, categories = result
            
            # Se tem múltiplas categorias, vai para 'diversos'
            if len(categories) > 1:
                self.stats['diversos'] += 1
                self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
                
                # Registra a combinação de categorias
                combo = '+'.join(sorted(categories))
                self.stats['diversos_combinations'][combo] = self.stats['diversos_combinations'].get(combo, 0) + 1
                
                # Armazena as categorias no item para uso posterior
                item['_categories'] = categories
                item['_primary_category'] = categories[0]
                
                return 'diversos'
            else:
                # Categoria única - tabela específica
                self.stats['success'] += 1
                self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
                return table_name
        
        # Fallback
        self.stats['failed'] += 1
        self.stats['total'] += 1
        return 'oportunidades'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[Tuple[str, List[str]]]:
        """
        Classifica com Groq e retorna (tabela_principal, lista_de_categorias)
        
        Returns:
            Tuple[str, List[str]] ou None se falhar
            - str: nome da tabela principal
            - List[str]: lista de todas as categorias aplicáveis
        """
        prompt = self._build_classification_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if response:
                # Parse da resposta - pode ser "tecnologia" ou "tecnologia,eletrodomesticos"
                response_clean = response.strip().lower()
                
                # Remove possíveis explicações extras
                if '\n' in response_clean:
                    response_clean = response_clean.split('\n')[0]
                
                # Separa múltiplas categorias
                categories = [cat.strip() for cat in response_clean.split(',')]
                
                # Valida todas as categorias
                valid_categories = [cat for cat in categories if cat in self.TABLES_INFO and cat not in ['diversos', 'oportunidades']]
                
                if valid_categories:
                    self.stats['success'] += 1
                    self.stats['total'] += 1
                    return valid_categories[0], valid_categories
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro na classificação: {e}")
            return None
    
    def _build_classification_prompt(self, title: str, description: str) -> str:
        """Monta o prompt para o Groq com suporte a múltiplas categorias"""
        
        # Lista de tabelas (excluindo diversos e oportunidades do prompt)
        tables_list = []
        for table, info in self.TABLES_INFO.items():
            if table not in ['diversos', 'oportunidades']:
                tables_list.append(f"- {table}: {info['desc']} (ex: {info['exemplos']})")
        
        tables_text = "\n".join(tables_list)
        
        prompt = f"""Você é um classificador de leilões. Analise o item e identifique TODAS as categorias que se aplicam.

CATEGORIAS DISPONÍVEIS:
{tables_text}

ITEM PARA CLASSIFICAR:
Título: {title}
Descrição: {description[:300] if description else 'Não disponível'}

REGRAS IMPORTANTES:
1. "veiculos" = QUALQUER forma de locomoção (bicicleta, patins, patinete, skate, scooter)
2. "nichados" = equipamentos especializados (odontológico, hospitalar, cozinha industrial, laboratório)
3. "eletrodomesticos" = apenas uso residencial (fogão doméstico, geladeira doméstica)
4. Se o item pertence a MÚLTIPLAS categorias, liste TODAS separadas por vírgula
5. Exemplos de múltiplas categorias:
   - Smart TV → tecnologia,eletrodomesticos
   - Air Fryer Wi-Fi → tecnologia,eletrodomesticos
   - Smartwatch → tecnologia,bens_consumo
   - Geladeira Inteligente → tecnologia,eletrodomesticos
6. Liste primeiro a categoria MAIS IMPORTANTE

RESPONDA APENAS COM AS CATEGORIAS (uma ou mais, separadas por vírgula):"""
        
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
                    "content": "Você é um classificador preciso. Responda com o nome da categoria ou múltiplas categorias separadas por vírgula. Sem explicações."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 100,
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
    
    def get_item_categories(self, item: Dict) -> Tuple[str, Optional[List[str]]]:
        """
        Retorna a tabela e as categorias de um item já classificado
        
        Returns:
            Tuple[str, Optional[List[str]]]: (tabela, lista_de_categorias)
        """
        # Primeiro classifica se ainda não foi
        if '_categories' not in item:
            table = self.classify(item)
            if table != 'diversos':
                return table, None
        
        # Se é diversos, retorna as categorias
        if '_categories' in item:
            return 'diversos', item['_categories']
        
        return 'oportunidades', None
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas de classificação"""
        return self.stats.copy()
    
    def print_stats(self):
        """Imprime estatísticas"""
        print("\n" + "="*70)
        print("📊 ESTATÍSTICAS DE CLASSIFICAÇÃO GROQ")
        print("="*70)
        print(f"Total processado: {self.stats['total']}")
        print(f"Sucesso (via Groq): {self.stats['success']} ({self.stats['success']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Auto-oportunidades: {self.stats['auto_oportunidades']} ({self.stats['auto_oportunidades']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"🎯 Diversos (múltiplas categorias): {self.stats['diversos']} ({self.stats['diversos']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"Falhas: {self.stats['failed']}")
        
        # Mostra motivos de oportunidades
        if self.stats['oportunidades_reasons']:
            print(f"\n💡 Motivos de Auto-Oportunidades:")
            for reason, count in sorted(self.stats['oportunidades_reasons'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['auto_oportunidades'] * 100 if self.stats['auto_oportunidades'] > 0 else 0
                print(f"  • {reason}: {count} ({pct:.1f}%)")
        
        # Mostra combinações de categorias em diversos
        if self.stats['diversos_combinations']:
            print(f"\n🎨 Combinações de Categorias (Diversos):")
            for combo, count in sorted(self.stats['diversos_combinations'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['diversos'] * 100 if self.stats['diversos'] > 0 else 0
                print(f"  • {combo}: {count} ({pct:.1f}%)")
        
        if self.stats['by_table']:
            # Organiza por pilar
            by_pillar = {1: {}, 2: {}, 3: {}, 4: {}}
            for table, count in self.stats['by_table'].items():
                pilar = self.TABLES_INFO.get(table, {}).get('pilar', 4)
                by_pillar[pilar][table] = count
            
            pilar_names = {
                1: "Pilar 1 (Varejo/Consumo)",
                2: "Pilar 2 (Casa/Decoração)",
                3: "Pilar 3 (Imóveis/Construção)",
                4: "Pilar 4 (Especialidades/Diversos)"
            }
            
            print(f"\n📦 Distribuição por Pilar e Tabela:")
            for pilar_num in [1, 2, 3, 4]:
                if by_pillar[pilar_num]:
                    pilar_total = sum(by_pillar[pilar_num].values())
                    pilar_pct = pilar_total / self.stats['total'] * 100
                    print(f"\n  🏛️  {pilar_names[pilar_num]}: {pilar_total} ({pilar_pct:.1f}%)")
                    for table, count in sorted(by_pillar[pilar_num].items(), key=lambda x: x[1], reverse=True):
                        pct = count / self.stats['total'] * 100
                        emoji = "🎯" if table == 'diversos' else "  "
                        print(f"      {emoji} {table}: {count} ({pct:.1f}%)")
        print("="*70)


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
    return classifier.classify(item) or 'oportunidades'


if __name__ == "__main__":
    # Teste focado em itens com múltiplas categorias
    classifier = GroqTableClassifier()
    
    test_items = [
        # ✅ ITENS COM MÚLTIPLAS CATEGORIAS (devem ir para 'diversos')
        {
            "title": "Smart TV Samsung 55' 4K com Wi-Fi",
            "description": "Televisão inteligente com sistema operacional e conectividade",
            "total_bids": 0
        },
        {
            "title": "Air Fryer Philips Walita com App e Wi-Fi",
            "description": "Fritadeira elétrica inteligente controlada por smartphone",
            "total_bids": 0
        },
        {
            "title": "Geladeira Brastemp Inverse com Alexa",
            "description": "Geladeira inteligente com assistente virtual integrado",
            "total_bids": 0
        },
        {
            "title": "Smartwatch Samsung Galaxy Watch 5",
            "description": "Relógio inteligente com múltiplas funções",
            "total_bids": 0
        },
        {
            "title": "Robô Aspirador Xiaomi com App",
            "description": "Aspirador robótico inteligente controlado por celular",
            "total_bids": 0
        },
        
        # ✅ ITENS DE CATEGORIA ÚNICA (devem ir para tabela específica)
        {
            "title": "Notebook Dell Inspiron 15",
            "description": "Notebook com 8GB RAM",
            "total_bids": 0
        },
        {
            "title": "Geladeira Consul 400L",
            "description": "Geladeira tradicional sem recursos inteligentes",
            "total_bids": 0
        },
        {
            "title": "Bicicleta Caloi Mountain Bike",
            "description": "Bicicleta aro 29",
            "total_bids": 0
        },
        
        # ✅ OPORTUNIDADES (com lances)
        {
            "title": "iPhone 13 Pro Max",
            "description": "Smartphone Apple",
            "total_bids": 5
        },
    ]
    
    print("\n🤖 TESTANDO CLASSIFICADOR COM MÚLTIPLAS CATEGORIAS\n")
    print("="*80)
    
    for item in test_items:
        table = classifier.classify(item)
        
        # Mostra as categorias se for 'diversos'
        categories_str = ""
        if table == 'diversos' and '_categories' in item:
            categories_str = f" → Categorias: {', '.join(item['_categories'])}"
        
        bids_info = f" [Lances: {item.get('total_bids', 0)}]" if item.get('total_bids', 0) > 0 else ""
        
        # Emoji baseado no resultado
        emoji = "🎯" if table == 'diversos' else "✅" if table != 'oportunidades' else "💡"
        
        print(f"{emoji} '{item['title'][:60]}'{bids_info}")
        print(f"   └─ Tabela: {table}{categories_str}\n")
    
    classifier.print_stats()