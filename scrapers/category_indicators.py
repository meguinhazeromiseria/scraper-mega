#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CATEGORY INDICATORS
📋 Indicadores e keywords para detecção de categorias
Separado para facilitar manutenção
"""

# ============================================================================
# TABELAS DISPONÍVEIS
# ============================================================================

TABLES_INFO = {
    # ========== VAREJO E CONSUMO ==========
    'tecnologia': {
        'desc': 'Eletrônicos e informática',
        'keywords': ['notebook', 'smartphone', 'tablet', 'computador', 'monitor', 
                    'impressora', 'impressoras', 'impressora digital', 'impressora portatil',
                    'camera', 'drone', 'console', 'videogame', 'xbox', 'playstation', 'nintendo',
                    'smartwatch', 'fone', 'headphone', 'caixa de som', 'roteador', 'switch',
                    'mouse', 'teclado', 'webcam', 'microfone', 'ssd', 'hd externo', 'pendrive',
                    'iphone', 'ipad', 'macbook', 'samsung galaxy', 'dell', 'lenovo', 'asus', 'acer',
                    'gopro', 'dji', 'canon', 'nikon', 'sony alpha', 'servidor', 'powervault',
                    'celular', 'moto g', 'galaxy', 'xiaomi', 'motorola', 'telefone celular',
                    'tekpix', 'zink', 'conexao usb', 'leitor de cartoes']
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
                    'carpete', 'decoracao', 'moldura', 'movel', 'moveis',
                    'cadeira escritorio', 'mesa escritorio', 'bancada escritorio',
                    'estante escritorio', 'arquivo', 'gaveteiro', 'mesa reuniao',
                    'cadeira giratoria', 'mesa diretoria', 'longarina']
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
                    'vergalhao', 'ferro', 'aco', 'colunas', 'vigas',
                    'cortadeira de piso', 'serra marmore', 'disco de corte']
    },
    'industrial_equipamentos': {
        'desc': 'Equipamentos industriais',
        'keywords': ['torno', 'fresadora', 'prensa', 'compressor', 'gerador',
                    'solda', 'transformador', 'motor industrial', 'bomba industrial',
                    'valvula industrial', 'maquina cnc', 'serra industrial',
                    'furadeira industrial', 'lixadeira industrial', 'esmerilhadeira',
                    'injetora', 'extrusora', 'caldeira', 'forno industrial',
                    'equipamento de producao', 'linha de producao', 'esteira',
                    'compactador', 'compactador de lixo', 'coletor de lixo',
                    'equipamento coleta', 'caminhao compactador']
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
        'desc': 'Equipamentos especializados (médico, odonto, veterinário, estética, cozinha profissional, farmácia)',
        'keywords': [
            # ODONTOLÓGICO
            'odontologico', 'odontologica', 'cadeira odontologica', 'raio-x dental',
            'raio x odontologico', 'autoclave', 'dentista', 'consultorio odontologico',
            'armario odontologico', 'bancada odontologica', 'mocho odontologico',
            'equipo odontologico', 'compressor odontologico', 'amalgamador',
            'fotopolimerizador', 'ultrassom odontologico', 'kavo', 'gnatus', 'dabi atlante',
            'odontologia', 'clinica odontologica', 'unidade odontologica',
            'material odontologico', 'instrumental odontologico',
            # MÉDICO/HOSPITALAR
            'medico', 'hospitalar', 'clinica', 'maca', 'mesa cirurgica',
            'bisturi', 'estetoscopio', 'equipamento medico', 'desfibrilador',
            'monitor de sinais', 'oximetro', 'esfigmomanometro',
            # FARMÁCIA/MEDICAMENTOS (NOVO!)
            'medicamento', 'medicamentos', 'farmacia', 'farmaceutico',
            'produto de higiene', 'higiene hospitalar', 'produto hospitalar',
            'vitamina', 'vitaminas', 'suplemento medicinal', 'remedio',
            'produto de saude', 'insumo medico', 'material hospitalar',
            # VETERINÁRIO
            'veterinario', 'maquina veterinaria', 'gaiola veterinaria',
            'mesa veterinaria', 'clinica veterinaria',
            # ESTÉTICA
            'estetica', 'depilacao laser', 'criolipilise', 'radiofrequencia',
            'ultracavitacao', 'microagulhamento', 'salon', 'spa',
            # COZINHA PROFISSIONAL/INDUSTRIAL
            'cozinha profissional', 'cozinha industrial', 'fogao industrial',
            'forno industrial', 'coifa industrial', 'chapa industrial',
            'fritadeira industrial', 'balcao refrigerado', 'camara fria',
            'freezer industrial', 'geladeira industrial', 'refrigerador industrial',
            'mesa inox', 'pia inox', 'bancada inox', 'fogao 6 bocas',
            'forno combinado', 'pass through', 'equipamento gastronomico',
            # LABORATÓRIO
            'laboratorio', 'centrifuga', 'microscopio', 'balanca analitica',
            'estufa laboratorio', 'capela de exaustao', 'autoclave laboratorio'
        ]
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


# ============================================================================
# INDICADORES PARA DETECÇÃO DE LOTES MISTOS
# ============================================================================

MIXED_LOT_CATEGORY_INDICATORS = {
    'tecnologia': ['notebook', 'tablet', 'smartphone', 'impressora', 'monitor', 'telefone', 'celular'],
    'eletrodomesticos': ['geladeira', 'fogao', 'microondas', 'micro-ondas', 'tv', 'televisao', 'lavadora', 'bebedouro'],
    'moveis': ['sofa', 'mesa', 'cadeira', 'armario', 'cama'],
    'casa_utilidades': ['panela', 'prato', 'copo', 'talher'],
    'veiculos': ['carro', 'moto', 'caminhao', 'bicicleta'],
    'imoveis': ['casa', 'apartamento', 'terreno']
}


# ============================================================================
# KEYWORDS FINANCEIROS/ABSTRATOS (sempre "diversos")
# ============================================================================

FINANCIAL_ABSTRACT_KEYWORDS = [
    'cotas sociais', 'acoes', 'ações', 'expectativa de direito',
    'direito creditorio', 'direitos creditorios', 'credito de',
    'emprestimo compulsorio', 'marca registrada', 'registro de marca',
    'marca devidamente registrada', 'inpi', 'titulo patrimonial',
    'titulo de clube', 'propriedade intelectual', 'patente'
]