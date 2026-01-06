#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GROQ TABLE CLASSIFIER v2.0 - Classificador Inteligente de Tabelas
🤖 Pré-classificador (70-80%) + Groq AI (20-30%) = 100% cobertura
✨ Cobre TODAS as 17 categorias + minimiza "diversos"
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
    """
    Classificador híbrido:
    1. Pré-classificador (keywords fortes) → 70-80% dos casos
    2. Groq AI (casos complexos) → 20-30% dos casos
    3. Fallback conservador → apenas casos impossíveis
    """
    
    def __init__(self):
        self.api_key = GROQ_API_KEY
        self.api_url = GROQ_API_URL
        
        if not self.api_key:
            raise ValueError("⚠️ GROQ_API_KEY não encontrada! Configure no .env")
        
        self.stats = {
            'total': 0,
            'pre_classified': 0,      # Pré-classificador
            'groq_classifications': 0, # Groq AI
            'financial_blocked': 0,    # Financeiros
            'mixed_detected': 0,       # Mistos
            'failed': 0,               # Fallback
            'by_table': {}
        }
    
    def _is_financial_abstract(self, item: Dict) -> bool:
        """
        Detecta itens financeiros/abstratos (sempre → diversos).
        Ex: ações, créditos, marcas, direitos, patentes
        """
        text = f"{item.get('normalized_title', '')} {item.get('description', '')}".lower()
        return any(kw in text for kw in FINANCIAL_ABSTRACT_KEYWORDS)
    
    def _is_obvious_mixed_lot(self, item: Dict) -> bool:
        """
        Detecta lotes EXPLICITAMENTE mistos no título.
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
    
    def _try_obvious_classification(self, title: str, description: str) -> Optional[str]:
        """
        PRÉ-CLASSIFICADOR: Detecta casos ÓBVIOS com keywords fortes.
        Cobre TODAS as 17 categorias. Economiza chamadas ao Groq.
        
        Retorna: categoria ou None (se não conseguir classificar)
        """
        text = f"{title} {description}".lower()
        
        # 1️⃣ IMÓVEIS (prioridade máxima - 80% dos casos no megaleiloes)
        imovel_kw = [
            'apartamento', 'casa ', 'terreno', 'lote ', 'sala comercial',
            'galpao', 'imovel', 'propriedade', ' m2', ' m²', 'metro quadrado',
            'quarto', 'suite', 'vaga ', 'garagem', 'fazenda', 'sitio', 'chacara',
            'edificio', 'cobertura', 'kitnet', 'studio', 'flat', 'condominio',
            'area rural', 'area urbana'
        ]
        if any(kw in text for kw in imovel_kw):
            # Evita falsos positivos (miniaturas, peças)
            if not any(x in text for x in ['peca', 'componente', 'miniatura', 'brinquedo']):
                return 'imoveis'
        
        # 2️⃣ VEÍCULOS (prioridade máxima - 10% dos casos)
        veiculo_kw = [
            'carro ', 'automovel', 'veiculo', ' moto ', 'motocicleta',
            'caminhao', 'onibus', ' van ', 'pickup', 'bicicleta', 'bike ',
            'jet ski', 'lancha', 'barco', 'aviao', 'aeronave', 'helicoptero',
            # Marcas comuns
            'fiat ', 'ford ', 'chevrolet', 'honda ', 'toyota', 'volkswagen',
            'hyundai', 'renault', 'nissan', 'peugeot',
            # Modelos comuns
            'civic', 'corolla', 'gol ', 'uno ', 'palio', 'onix', 'hb20',
            'sandero', 'logan', 'cg 150', 'cg 160', 'fan ', 'titan'
        ]
        if any(kw in text for kw in veiculo_kw):
            # Evita peças avulsas
            if not any(x in text for x in ['peca', 'motor (peca)', 'bateria (peca)', 'miniatura']):
                return 'veiculos'
        
        # 3️⃣ NICHADOS (alta prioridade - equipamentos profissionais)
        nichado_kw = [
            # Farmácia/Medicamentos
            'medicamento', 'farmacia', 'farmaceutico', 'produto de higiene',
            'higiene hospitalar', 'produto hospitalar', 'vitamina',
            'material hospitalar', 'insumo medico',
            # Odontológico
            'odontologic', 'cadeira odontologic', 'dentista', 'consultorio odontologico',
            'equipo odontologico', 'autoclave', 'raio x dental', 'kavo', 'gnatus',
            # Médico/Hospitalar
            'equipamento medico', 'hospitalar', 'maca', 'mesa cirurgica',
            'desfibrilador', 'monitor de sinais', 'clinica',
            # Veterinário
            'veterinario', 'clinica veterinaria', 'mesa veterinaria',
            # Estética
            'depilacao laser', 'criolipilise', 'radiofrequencia', 'estetica profissional',
            # Cozinha Industrial
            'fogao industrial', 'geladeira industrial', 'refrigerador industrial',
            'cozinha industrial', 'cozinha profissional', 'forno industrial',
            'fogao 6 bocas', 'coifa industrial', 'camara fria', 'freezer industrial',
            'balcao refrigerado', 'mesa inox', 'pia inox', 'bancada inox',
            'equipamento gastronomico', 'pass through',
            # Laboratório
            'laboratorio', 'centrifuga', 'microscopio', 'balanca analitica'
        ]
        if any(kw in text for kw in nichado_kw):
            return 'nichados'
        
        # 4️⃣ TECNOLOGIA
        tech_kw = [
            'notebook', 'computador', 'impressora', 'smartphone', 'celular',
            'tablet', 'iphone', 'ipad', 'samsung galaxy', 'servidor',
            'monitor ', 'camera digital', 'drone ', 'videogame', 'console',
            'xbox', 'playstation', 'smartwatch', 'roteador', 'switch ',
            'mouse', 'teclado', 'webcam', 'ssd ', 'hd externo', 'pendrive'
        ]
        if any(kw in text for kw in tech_kw):
            return 'tecnologia'
        
        # 5️⃣ ELETRODOMÉSTICOS
        eletro_kw = [
            'geladeira', 'refrigerador', 'fogao ', 'microondas', 'micro-ondas',
            'lavadora', 'secadora', 'lava e seca', 'ar condicionado',
            'ventilador', 'purificador', ' tv ', 'televisao', 'smart tv',
            'air fryer', 'fritadeira eletrica', 'aspirador', 'cafeteira',
            'liquidificador', 'batedeira', 'ferro de passar'
        ]
        if any(kw in text for kw in eletro_kw):
            # Valida que NÃO é industrial
            if not any(x in text for x in ['industrial', '6 bocas', 'profissional', 'inox']):
                return 'eletrodomesticos'
        
        # 6️⃣ MÓVEIS E DECORAÇÃO
        moveis_kw = [
            'sofa', 'mesa ', 'cadeira', 'poltrona', 'armario', 'guarda-roupa',
            'cama ', 'colchao', 'estante', 'rack ', 'criado-mudo', 'comoda',
            'aparador', 'buffet', 'escrivaninha', 'puff', 'banqueta',
            'lustres', 'luminaria', 'quadro decoracao', 'espelho', 'tapete',
            'cortina', 'persiana', 'carpete'
        ]
        if any(kw in text for kw in moveis_kw):
            return 'moveis_decoracao'
        
        # 7️⃣ CASA UTILIDADES
        utilidades_kw = [
            'panela', 'frigideira', 'assadeira', 'prato', 'tigela', 'bowl',
            'talher', 'garfo', 'faca ', 'colher', 'copo ', 'xicara', 'caneca',
            'jarra', 'marmita', 'pote ', 'organizador domestico', 'cesto',
            'vassoura', 'rodo', 'balde', 'varal', 'tabua de corte',
            'kit churrasco'
        ]
        if any(kw in text for kw in utilidades_kw):
            return 'casa_utilidades'
        
        # 8️⃣ BENS DE CONSUMO
        consumo_kw = [
            'roupa', 'calcado', 'sapato', 'tenis', 'bolsa', 'mochila',
            'carteira', 'oculos', 'relogio', 'joia', 'colar', 'anel',
            'brinco', 'pulseira', 'perfume', 'cosmetico', 'maquiagem',
            'mala ', 'valise', 'bone ', 'chapeu', 'cachecol', 'cinto'
        ]
        if any(kw in text for kw in consumo_kw):
            return 'bens_consumo'
        
        # 9️⃣ ALIMENTOS E BEBIDAS
        alimentos_kw = [
            'vinho', 'whisky', 'cerveja', 'cafe ', 'cha ', 'suco ',
            'refrigerante', 'agua mineral', 'suplemento alimentar',
            'proteina', 'whey', 'barra de cereal', 'chocolate'
        ]
        if any(kw in text for kw in alimentos_kw):
            return 'alimentos_bebidas'
        
        # 🔟 MATERIAIS DE CONSTRUÇÃO
        construcao_kw = [
            'cimento', 'tijolo', 'bloco', 'telha', 'piso ', 'porcelanato',
            'ceramica', 'azulejo', 'revestimento', 'porta ', 'janela',
            'fechadura', 'tinta ', 'verniz', 'tubo ', 'cano ', 'torneira',
            'registro', 'madeira', 'tabua ', 'viga', 'areia ', 'brita',
            'vergalhao', 'ferro ', 'aco ',
            # Ferramentas de construção
            'cortadeira de piso', 'serra marmore', 'disco de corte',
            'furadeira', 'parafusadeira', 'nivel', 'prumo'
        ]
        if any(kw in text for kw in construcao_kw):
            return 'materiais_construcao'
        
        # 1️⃣1️⃣ INDUSTRIAL EQUIPAMENTOS
        industrial_kw = [
            'torno', 'fresadora', 'prensa', 'compressor industrial',
            'gerador', 'transformador', 'motor industrial',
            'bomba industrial', 'maquina cnc', 'serra industrial',
            'furadeira industrial', 'lixadeira industrial',
            'esmerilhadeira', 'injetora', 'extrusora', 'caldeira',
            'forno industrial', 'equipamento de producao', 'linha de producao',
            'esteira transportadora', 'compactador', 'compactador de lixo',
            'coletor de lixo', 'caminhao compactador'
        ]
        if any(kw in text for kw in industrial_kw):
            return 'industrial_equipamentos'
        
        # 1️⃣2️⃣ MÁQUINAS PESADAS E AGRÍCOLAS
        maquinas_kw = [
            'retroescavadeira', 'escavadeira', 'pa carregadeira',
            'motoniveladora', 'rolo compactador', 'patrol',
            'trator agricola', 'colheitadeira', 'plantadeira',
            'pulverizador', 'grade agricola', 'arado', 'semeadeira',
            'rocadeira', 'empilhadeira', 'bobcat', 'minicarregadeira',
            'terraplenagem'
        ]
        if any(kw in text for kw in maquinas_kw):
            return 'maquinas_pesadas_agricolas'
        
        # 1️⃣3️⃣ PARTES E PEÇAS
        pecas_kw = [
            'peca ', 'pecas ', 'componente', 'reposicao', 'sobressalente',
            'motor (peca)', 'engrenagem', 'rolamento', 'correia',
            'filtro ', 'vela ', 'bateria (peca)', 'alternador',
            'radiador', 'pneu', 'aro ', 'disco de freio', 'pastilha',
            'amortecedor', 'suspensao', 'cambio (peca)', 'embreagem'
        ]
        if any(kw in text for kw in pecas_kw):
            return 'partes_pecas'
        
        # 1️⃣4️⃣ ANIMAIS
        animais_kw = [
            'gado', ' boi ', ' vaca ', 'novilho', 'touro', 'cavalo',
            'egua', 'potro', 'jumento', 'porco', 'suino', 'galinha',
            'frango', 'pato', 'ovelha', 'carneiro', 'cabra', 'caprino',
            'ovino', 'ave ', 'animal vivo', 'plantel'
        ]
        if any(kw in text for kw in animais_kw):
            return 'animais'
        
        # 1️⃣5️⃣ SUCATAS E RESÍDUOS
        sucatas_kw = [
            'sucata', 'residuo', 'reciclavel', 'descarte', 'ferro velho',
            'metal sucata', 'aluminio sucata', 'cobre sucata', 'lata',
            'papelao', 'plastico sucata', 'eletronica sucata',
            'bateria usada', 'aparas', 'refugo', 'resto', 'sobra'
        ]
        if any(kw in text for kw in sucatas_kw):
            return 'sucatas_residuos'
        
        # 1️⃣6️⃣ ARTES E COLECIONISMO
        artes_kw = [
            'quadro arte', 'pintura', 'escultura', 'estatua',
            'obra de arte', 'antiguidade', 'moeda antiga', 'selo',
            'colecao', 'colecionavel', 'raridade', 'vintage',
            'retro', 'reliquia', 'porcelana antiga', 'cristal antigo'
        ]
        if any(kw in text for kw in artes_kw):
            return 'artes_colecionismo'
        
        # Não conseguiu classificar com keywords → Groq decide
        return None
    
    def classify(self, item: Dict) -> Optional[str]:
        """
        FLUXO PRINCIPAL DE CLASSIFICAÇÃO:
        
        1. Bloqueia financeiros/abstratos → diversos (1-2%)
        2. Detecta mistos óbvios → diversos (0-1%)
        3. Pré-classificador (keywords) → categoria específica (70-80%)
        4. Groq AI (casos complexos) → categoria específica (15-25%)
        5. Fallback conservador → diversos (apenas impossíveis)
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
        
        # 2️⃣ DETECTA MISTOS EXPLÍCITOS
        if self._is_obvious_mixed_lot(item):
            self.stats['mixed_detected'] += 1
            self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
            self.stats['total'] += 1
            
            if self.stats['mixed_detected'] <= 3:
                print(f"  🎨 DIVERSOS (misto): '{title[:60]}'")
            
            return 'diversos'
        
        # 3️⃣ PRÉ-CLASSIFICADOR (keywords fortes - rápido)
        obvious_category = self._try_obvious_classification(title, description)
        if obvious_category:
            self.stats['pre_classified'] += 1
            self.stats['by_table'][obvious_category] = self.stats['by_table'].get(obvious_category, 0) + 1
            self.stats['total'] += 1
            
            # Log apenas primeiros 5 de cada categoria
            category_count = self.stats['by_table'][obvious_category]
            if category_count <= 5:
                print(f"  🎯 {obvious_category}: '{title[:55]}'")
            
            return obvious_category
        
        # 4️⃣ GROQ AI (casos complexos)
        table_name = self._classify_with_groq(title, description)
        
        if table_name:
            self.stats['groq_classifications'] += 1
            self.stats['by_table'][table_name] = self.stats['by_table'].get(table_name, 0) + 1
            self.stats['total'] += 1
            
            if self.stats['groq_classifications'] <= 10:
                print(f"  🤖 {table_name}: '{title[:55]}'")
            
            return table_name
        
        # 5️⃣ FALLBACK (último recurso)
        self.stats['failed'] += 1
        self.stats['by_table']['diversos'] = self.stats['by_table'].get('diversos', 0) + 1
        self.stats['total'] += 1
        
        if self.stats['failed'] <= 3:
            print(f"  ⚠️ FALLBACK diversos: '{title[:55]}'")
        
        return 'diversos'
    
    def _classify_with_groq(self, title: str, description: str) -> Optional[str]:
        """Classifica com Groq AI + validação forte"""
        prompt = self._build_smart_prompt(title, description)
        
        try:
            response = self._call_groq(prompt)
            
            if not response:
                return None
            
            # Limpa resposta
            response_clean = response.strip().lower()
            response_clean = response_clean.replace('\n', ' ').replace(',', '').replace(';', '').replace('.', '')
            response_clean = response_clean.split()[0] if response_clean else ''
            
            # Validação 1: categoria exata
            if response_clean in TABLES_INFO:
                return response_clean
            
            # Validação 2: mapeamento de variações
            mappings = {
                # Imóveis
                'imovel': 'imoveis', 'propriedade': 'imoveis',
                'casa': 'imoveis', 'apartamento': 'imoveis',
                # Veículos
                'veiculo': 'veiculos', 'carro': 'veiculos', 'moto': 'veiculos',
                # Tech & Eletro
                'tecnologias': 'tecnologia', 'tech': 'tecnologia',
                'eletrodomestico': 'eletrodomesticos', 'eletro': 'eletrodomesticos',
                # Casa
                'movel': 'moveis_decoracao', 'moveis': 'moveis_decoracao',
                'utilidades': 'casa_utilidades', 'utilidade': 'casa_utilidades',
                # Consumo
                'consumo': 'bens_consumo', 'bens': 'bens_consumo',
                'alimento': 'alimentos_bebidas', 'bebida': 'alimentos_bebidas',
                # Construção & Industrial
                'construcao': 'materiais_construcao', 'material': 'materiais_construcao',
                'industrial': 'industrial_equipamentos', 'equipamento': 'industrial_equipamentos',
                'maquina': 'maquinas_pesadas_agricolas', 'maquinas': 'maquinas_pesadas_agricolas',
                'agricola': 'maquinas_pesadas_agricolas', 'agricolas': 'maquinas_pesadas_agricolas',
                # Outros
                'nichado': 'nichados', 'peca': 'partes_pecas', 'pecas': 'partes_pecas',
                'animal': 'animais', 'sucata': 'sucatas_residuos',
                'arte': 'artes_colecionismo', 'colecionismo': 'artes_colecionismo',
            }
            
            if response_clean in mappings:
                return mappings[response_clean]
            
            return None
        
        except Exception as e:
            print(f"⚠️ Erro Groq: {e}")
            return None
    
    def _build_smart_prompt(self, title: str, description: str) -> str:
        """Prompt DIRETO cobrindo TODAS as 17 categorias"""
        
        prompt = f"""Você é um classificador de leilões. Classifique este item na categoria MAIS ESPECÍFICA.

ITEM:
Título: {title}
Descrição: {description[:300] if description else 'N/A'}

================================================================================
CATEGORIAS (17 opções)
================================================================================

🏠 GRANDES ATIVOS:
  • imoveis → casa, apartamento, terreno, lote, sala, galpão, fazenda
  • veiculos → carro, moto, caminhão, ônibus, barco, avião, bicicleta

💻 TECNOLOGIA & ELETRO:
  • tecnologia → notebook, celular, impressora, tablet, câmera, drone, servidor
  • eletrodomesticos → geladeira, fogão, microondas, TV, ar condicionado, lavadora

🛋️ CASA & DECORAÇÃO:
  • moveis_decoracao → sofá, mesa, cadeira, armário, cama, estante
  • casa_utilidades → panela, prato, copo, talher, organizador, vassoura
  • artes_colecionismo → quadros, esculturas, antiguidades, obras de arte

🍔 CONSUMO:
  • bens_consumo → roupas, calçados, bolsas, óculos, relógios, joias, perfumes
  • alimentos_bebidas → vinho, café, suplementos

🏗️ CONSTRUÇÃO & INDUSTRIAL:
  • materiais_construcao → cimento, tijolo, piso, tinta, ferramentas de construção
  • industrial_equipamentos → torno, prensa, compressor, gerador, compactador de lixo
  • maquinas_pesadas_agricolas → trator, escavadeira, colheitadeira, retroescavadeira

🏥 ESPECIALIDADES:
  • nichados → equipamentos médicos, odontológicos, farmácia, veterinário, cozinha industrial

🔧 OUTROS:
  • partes_pecas → peças avulsas, componentes, reposição
  • animais → gado, cavalos, aves vivas
  • sucatas_residuos → sucata, reciclável, descarte

🎨 DIVERSOS:
  • diversos → APENAS: itens abstratos (ações, créditos, marcas) OU lotes mistos explícitos

================================================================================
REGRAS (SIGA ESTA ORDEM)
================================================================================

1. IMÓVEIS: qualquer propriedade → "imoveis"
2. VEÍCULOS: qualquer transporte → "veiculos" (exceto peças → "partes_pecas")
3. NICHADOS: equipamento profissional (farmácia, hospital, dentista, cozinha industrial) → "nichados"
4. MÁQUINAS: equipamento industrial/agrícola → "industrial_equipamentos" ou "maquinas_pesadas_agricolas"
5. MÓVEIS: você SENTA/GUARDA/DECORA → "moveis_decoracao"
6. UTILIDADES: você USA para cozinhar/comer/limpar → "casa_utilidades"
7. TECH: informática, comunicação → "tecnologia"
8. ELETRO: linha branca, TV → "eletrodomesticos"
9. DIVERSOS: APENAS se abstrato ou lote misto explícito

RESPONDA APENAS A CATEGORIA (ex: "imoveis", "veiculos", "tecnologia"):"""
        
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
                    "content": "Você é um classificador EXPERT em leilões. Analise o CONTEXTO e a FUNÇÃO REAL do item. Responda APENAS o nome da categoria."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.05,
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
            print(f"⚠️ Erro Groq: {e}")
            return None
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas"""
        return self.stats.copy()
    
    def print_stats(self):
        """Imprime estatísticas detalhadas"""
        print("\n" + "="*80)
        print("📊 ESTATÍSTICAS DE CLASSIFICAÇÃO v2.0")
        print("="*80)
        print(f"Total processado: {self.stats['total']}")
        print()
        print("📍 MÉTODOS DE CLASSIFICAÇÃO:")
        print(f"  🎯 Pré-classificador (keywords): {self.stats['pre_classified']} ({self.stats['pre_classified']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"  🤖 Groq AI (casos complexos):   {self.stats['groq_classifications']} ({self.stats['groq_classifications']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"  💼 Financeiros bloqueados:      {self.stats['financial_blocked']} ({self.stats['financial_blocked']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"  🎨 Mistos detectados:           {self.stats['mixed_detected']} ({self.stats['mixed_detected']/max(self.stats['total'],1)*100:.1f}%)")
        print(f"  ⚠️  Fallback (diversos):         {self.stats['failed']} ({self.stats['failed']/max(self.stats['total'],1)*100:.1f}%)")
        
        if self.stats['by_table']:
            print(f"\n📦 DISTRIBUIÇÃO POR TABELA:")
            print("-" * 80)
            
            for table, count in sorted(self.stats['by_table'].items(), key=lambda x: x[1], reverse=True):
                pct = count / self.stats['total'] * 100
                bar = "█" * int(pct / 2)
                emoji = "🎨" if table == 'diversos' else "  "
                print(f"{emoji} {table:.<35} {count:>6} ({pct:>5.1f}%) {bar}")
        
        print("="*80)
        
        # Análise de eficiência
        pre_pct = self.stats['pre_classified']/max(self.stats['total'],1)*100
        groq_pct = self.stats['groq_classifications']/max(self.stats['total'],1)*100
        diversos_pct = self.stats['by_table'].get('diversos', 0)/max(self.stats['total'],1)*100
        
        print(f"\n💡 ANÁLISE DE EFICIÊNCIA:")
        print(f"   • Pré-classificador: {pre_pct:.1f}% (ótimo se >70%)")
        print(f"   • Groq AI: {groq_pct:.1f}% (ideal entre 15-30%)")
        print(f"   • Diversos: {diversos_pct:.1f}% (ótimo se <5%)")
        
        if diversos_pct > 10:
            print(f"   ⚠️  ATENÇÃO: 'diversos' muito alto ({diversos_pct:.1f}%)!")
            print(f"      → Adicione keywords no pré-classificador")
        elif diversos_pct < 5:
            print(f"   ✅ Excelente! 'diversos' está controlado ({diversos_pct:.1f}%)")


def classify_item_to_table(item: Dict) -> str:
    """Função auxiliar: classifica um item"""
    classifier = GroqTableClassifier()
    return classifier.classify(item) or 'diversos'


if __name__ == "__main__":
    # TESTES
    print("\n🧪 TESTE - CLASSIFICADOR v2.0 COMPLETO\n")
    print("="*80)
    print("Pré-classificador + Groq AI = 100% cobertura (17 categorias)")
    print("="*80 + "\n")
    
    classifier = GroqTableClassifier()
    
    test_items = [
        # IMÓVEIS
        {"normalized_title": "apartamento 53 m2 01 vaga parque das nacoes", "description": ""},
        {"normalized_title": "casa 131 m2 novo jardim patente sao paulo", "description": ""},
        {"normalized_title": "terreno 300 m2 zona sul", "description": ""},
        
        # VEÍCULOS
        {"normalized_title": "carro volkswagen gol 2015", "description": ""},
        {"normalized_title": "moto honda cg 150", "description": ""},
        {"normalized_title": "caminhao mercedes 710", "description": ""},
        
        # DIVERSOS (financeiros)
        {"normalized_title": "5948 acoes preferenciais classe b elet6", "description": ""},
        {"normalized_title": "marca registrada no inpi", "description": ""},
    ]
    
    print("🔍 CLASSIFICANDO ITENS DE TESTE...\n")
    
    for i, item in enumerate(test_items, 1):
        table = classifier.classify(item)
        print(f"{i}. '{item['normalized_title'][:50]}' → {table}")
    
    classifier.print_stats()