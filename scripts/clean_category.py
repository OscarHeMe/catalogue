import os
import pandas as pd
import requests
import re
import logging
from tqdm import tqdm_notebook
from pygres import Pygres
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import unicodedata
import string


exclude = ["a", "ante", "con", "de", "desde", "hacia", "para", "por", "si", "so", "tras", "el", "la",
           "los", "las", "un", "una", "unos", "unas", "aquel", "aquellos", "aquella", "aquello", "aquellas",
           "cuanto", "como", "que", "se", "cuesta", "precio", "y", "accesorios"]

exclude_name = ['super']

def clean(categories, is_name=False):
    if isinstance(categories, list):
        categories = ' '.join(categories)
    category_aux = ''.join(
                x for x in unicodedata.normalize('NFKD', categories) if x in string.ascii_letters or x in [" ", "-", "_", "."]).lower()
    category_aux = re.sub('[^a-z]', ' ', category_aux)
    category_aux = re.sub(' +', ' ', category_aux)
    if is_name:
        category_aux = ' '.join([word for word in list(set(category_aux.split(" "))) if word not in exclude + exclude_name and len(word) > 1])
    else:
        category_aux = ' '.join([word for word in list(set(category_aux.split(" "))) if word not in exclude and len(word) > 3])
    return category_aux

def to_key(categories):
    category_aux = ''.join(
                x for x in unicodedata.normalize('NFKD', categories) if x in string.ascii_letters or x in [" ", "-", "_", "."]).lower()
    category_aux = re.sub('[^a-z]', ' ', category_aux)
    category_aux = re.sub(' +', ' ', category_aux)
    return category_aux


# SPECIAL TOKENS
entretenimiento  = ['novela', 'drama', 'terror', 'miedo', 'ciencia ficcion', 'pelicula', 'cd', 'dvd', 'blu ray',
                    'musica', 'video', 'fotografia', 'camara', 'libro', 'television', 'radio', 'literatura', 'cuento']

farmacia = ['cardiobascular', 'bacteria', 'infeccion', 'antiseptico', 'moreton', 'cortadura', 'inmuno',
            'hormonal', 'farmaco', 'hipertension', 'diabetes', 'hospital', 'pediatra', 'doctor']


cuidado_personal_belleza = ['belleza', 'maquillaje', 'rubor', 'mascarilla', 'pies',
                    'repelente', 'cuerpo', 'diente', 'boca', 'bucal', 'higiene', 'tinte',
                    'desodorante', 'perfume', 'cosmetico', 'arruga', 'acne', 'faja' ,
                    'depilar', 'afeitar', 'cabello', 'capilar', 'shampoo', 'acondicionador',
                    'esencia', 'locion', 'fragancia', 'repelente', 'champu', 'caspa',
                    'cepillo', 'alicata', 'cortauna', 'lima', 'labio', 'talco', 'fragancia',
                    'pestana', 'antitranspirante', 'garnier', 'nivea', 'pantene',
                    'gillete', 'revlon', 'esmalte', 'fijador', 'moldeador']

ropa_zapatos_accesorios = ['ropa', 'zapato', 'calzado', 'reloj', 'joyeria' 'pulsera', 'anillo', 'oro', 'lente',
                             'vesitdo', 'gorra', 'chamarra', 'playera', 'sueter', 'sudadera', 'falda', 'tennis',
                             'pantalon', 'short', 'bermuda', 'pantufla', 'jeans', 'camisa', 'sombrero', 'jersey',
                             'brasier', 'calzon', 'calcetin', 'collar']

super_ = ['super', 'hamburguesa', 'hot-dog', 'alimento', 'pizza', 'fiesta', 'pastel', 'cafe']

cerveza_vinos_licores = ['cerveza', 'vino', 'whisky', 'licor', 'alcohol', 'tequila', 'champagne', 'vodka',
                           'mezcal', 'brandy', 'cognac', 'hielo', 'destilado', 'sidra', 'clericot']

# CATEGORIES
# Farma

mama_bebe_t = ['bebe', 'mama', 'infantil', 'materna', 'gerber', 'papilla', 'lactancia', 'panal', 'huggies',
               'pampers', 'biberon', 'dodot', 'baby']
mama_bebe_b = ['miel', 'abeja', 'cafe', 'refresco', 'detergente', 'bebida'] + entretenimiento + cerveza_vinos_licores

medicamentos_t = ['antiacuagulante', 'antiestaminico', 'antidepresivo', 'antiviral', 'antigripal', 'antidiarreico',
                  'medicamento', 'analgesico', 'desparasitante', 'alta especialidad', 'generico', 'gripa',
                  'antiespasmodico', 'antiniinflamatorio', 'antiagregante', 'antidiabeticos', 'antiemeticos',
                  'alergia', 'dolor', 'ampolleta', 'inyeccion', 'antimicoticos']
medicamentos_b = ['ortopedia'] + entretenimiento

naturales_t = ['natural', 'naturista', 'herbolario']
naturales_b = [] + entretenimiento

salud_sexual_t = ['tampon', 'intima', 'sexual', 'condon', 'preservativo', 'embarazo', 'vagina', 'vibrador',
                  'lubricante', 'estimulante', 'anticonceptivo', 'toalla']
salud_sexual_b = ['bebe', 'bano', 'alberca', 'cuerpo', 'cara', 'aceite', 'papel', 'ojos', 'nariz', 'nasal', 'oftal',
                  'laxante'] + entretenimiento

equipo_botiquin_t = ['equipo medico', 'oximetro', 'baumanometro', 'termometro', 'botiquin', 'jeringa', 'nebulizador', 'alcohol', 'anticeptico',
                                   'gasa', 'venda', 'vendita', 'bandita', 'algodon', 'hisopo', 'curita',
                                   'oxigenada', 'suero', 'aparato', 'guante latex']
equipo_botiquin_b = [] + entretenimiento + ropa_zapatos_accesorios

derma_t = ['piel', 'cara', 'bloqueador', 'derma']
derma_b = ['lacteo', 'huevo', 'leche', 'bebida', 'alimento', 'abarrote', 'despensa'] + entretenimiento

vitaminas_suplementos_t = ['vitamina', 'vitaminico', 'suplemento']
vitaminas_suplementos_b = [] + entretenimiento



#Ambos
# Ambos
cuidado_personal_belleza_t = derma_t + cuidado_personal_belleza
cuidado_personal_belleza_b = ['lacteo', 'huevo', 'leche', 'disfraz', 'perro', 'mascota', 'bebida', 'alimento',
                              'abarrote', 'despensa', 'caramelo'] + entretenimiento

# Super
frutas_verduras_t = ['fruta', 'verdura', 'ensalada', 'lechuga', 'manojo', 'tuberculo']
frutas_verduras_b = ['farmacia', 'preparado', 'lata'] + entretenimiento

panaderia_tortilleria_t = ['panaderia', 'dona', 'tortilla', 'maiz', 'harina', 'rosca', 'panque', 'bolillo', 'muffin']
panaderia_tortilleria_b = ['farmacia', 'pantalla', 'pantalon', 'ropa'] + entretenimiento

botanas_dulces_t = ['dulce', 'botana', 'nacho', 'dip', 'chicle', 'paleta', 'chocolate', 'papas', 'fritas',
                    'cacahuate', 'sabritas', 'doritos', 'cheetos', 'palomita', 'caramelo', 'goma mascar', 'chamoy',
                    'salsa valentina', 'salsa maggy', 'salsa inglesa']
botanas_dulces_b = ['farmacia', 'verdura', 'olor', 'color', 'desodorante', 'aromatizante'] + entretenimiento + cuidado_personal_belleza

carnes_pescados_t = ['carne', 'ternera', 'puerco', 'cerdo', 'pulpo', 'pescado', 'camaron', 'pulpo']
carnes_pescados_b = ['farmacia', 'lata'] + entretenimiento

lacteos_huevo_t = ['leche', 'huevo', 'yogurt', 'yoghurt', 'yugur', 'lacteo', 'helado', 'flan', 'natilla', 'nata']
lacteos_huevo_b = ['farmacia', 'higiene', 'cuerpo', 'mano', 'dental', 'oral'] + entretenimiento

salchichoneria_quesos_gourmet_t = ['salchicha', 'jamon', 'queso', 'gourmet', 'pavo', 'platillo']
salchichoneria_quesos_gourmet_b = ['farmacia'] + entretenimiento

alimentos_congelados_regrigerados_t = ['congelado', 'refrigerado', 'gelatina', 'flan', 'natilla', 'nata', 'hielo']
alimentos_congelados_regrigerados_b = ['farmacia', 'polvo'] + entretenimiento

jugos_bebidas_t = ['jugo', 'agua', 'refresco', 'leche', 'yogurt', 'bebida', 'bebible', 'hielo', 'jumex', 'boing',
                   'cocacola', 'pepsi', 'fanta', 'squirt']
jugos_bebidas_b = ['farmacia', 'polvo', 'oxigenada', 'infantil', 'lactea', 'calentador', 'perfume', 'locion', 'perro',
                   'gato', 'cachorro'] + entretenimiento + cerveza_vinos_licores

despensa_t = ['miel', 'mermelada', 'avena', 'cafe', 'aceite','atun', 'sopa', 'pasta', 'despensa', 'abarrote',
              'alimento', 'lata', 'cereal', 'galleta', 'azucar', 'especia', 'sazonador', 'chile',
              'salsa', 'semilla', 'mayoneza', 'aderezo', 'mayonesa', 'herdez', 'tuny', 'costeña', 'del monte',
              'nestle', 'kellogs', 'gelatina', 'flan', 'postre'
              ] + panaderia_tortilleria_t + lacteos_huevo_t + jugos_bebidas_t
despensa_b = ['diente', 'farmacia', 'cabello', 'cuerpo', 'pescado', 'fresco', 'mascota', 'perro', 'gato'] + entretenimiento

desechables_t = ['clinex', 'plato', 'vaso', 'servilleta', 'desechable', 'cuchillo', 'cuchara', 'tenedor',
                 'cubiertos', 'higienico', 'panuelo']
desechables_b = ['metal', 'cuaderno'] + entretenimiento

jugueteria_t = ['juguete', 'juego', 'muneco', 'pelota', 'burbuja', 'peluche']
jugueteria_b = ['farmacia', 'bano'] + entretenimiento

ferreteria_jarceria_t = ['ferreteria', 'jarceria', 'herramienta', 'truper', 'martillo', 'taladro', 'bombilla', 'foco']
ferreteria_jarceria_b = ['farmacia'] + entretenimiento

mascotas_t = ['mascota', 'perro', 'peces', 'gato', 'pajaro', 'tortuga', 'cachorro', 'antipulga', 'morder', 'carnaza']
mascotas_b = ['farmacia'] + entretenimiento

hogar_t = ['hogar', 'blancos', 'bano', 'comedor', 'sala', 'mueble', 'silla', 'cocina', 'casa', 'domestico', 'cojin',
           'sabana', 'mesa', 'patio', 'adorno', 'plato', 'cubierto', 'cubiertos', 'taza', 'jardin', 'iluminacion',
           'lampara', 'vajilla', 'olla', 'tocador', 'tapete', 'alfombra']
hogar_b = ['farmacia', 'mascota', 'perro', 'gato', 'carro', 'auto'] + entretenimiento

limpieza_detergentes_t = ['limpieza', 'jabon', 'detergente', 'manchas', 'aromatizante']
limpieza_detergentes_b = ['farmacia'] + entretenimiento

entretenimiento_t = ['arte', 'telescopio', 'baraja', 'casino', 'juegos', 'consola', 'nintendo', 'play station',
                     'xbox', 'hasbro', 'matel', 'muneco', 'platilina'] + entretenimiento
entretenimiento_b = ['farmacia']

computo_electronica_t = ['tecnologia', 'electronico', 'celular', 'telefono', 'computo', 'computadora', 'pc',
                         'teclado', 'monitor', 'pantalla', 'bocina', 'cargador', 'usb', 'game',
                         'gps', 'phone', 'huawei', 'sony', 'samsung', 'panasonic', 'microsoft',
                         'software', 'lenovo', 'kingston', 'dell', 'thoshiba', 'intel', 'toshiba', 'bocina',
                         'estereo', 'display', 'mouse', 'inalambrico', 'nvidia']
computo_electronica_b = ['farmacia', 'mickey', 'mascota'] + entretenimiento

autos_motos_llantas_t = ['moto', 'auto', 'carro', 'llanta']
autos_motos_llantas_b = ['farmacia', 'autores', 'ferreteria'] + entretenimiento

deportes_t = ['deporte', 'pesas', 'futbol', 'football', 'alberca', 'natacion', 'baseball', 'beisbol', 'basquetbol',
              'basketball', 'volibol', 'volleyball', 'pelota', 'balon', 'gimnasio', 'gym', 'caminadora', 'bicicleta',
              'ciclismo', 'raqueta', 'nike', 'adidas', 'nfl', 'nba', 'goggles']
deportes_b = ['farmacia'] + entretenimiento

oficina_papeleria_t = ['oficina', 'cuaderno', 'lapiz', 'lapices', 'lapicera', 'gis', 'colores', 'pluma', 'plumon',
                       'pepeleria','papel', 'confeti', 'globo', 'regalo', 'tarjeta', 'serpentina', 'libreta', 'fiesta',
                       'engrapadora', 'perforadora', 'cartulina', 'monografia', 'boligrafo', 'hoja']
oficina_papeleria_b = ['farmacia', 'higienico', 'bano', 'labial', 'belleza', 'pestana', 'delineador', 'maquillaje',
                       'ojo', 'labio', 'ganso', 'cojin', 'cobija', 'almohada', 'pavo'] + entretenimiento

cerveza_vinos_licores_t = cerveza_vinos_licores
cerveza_vinos_licores_b = ['farmacia', 'etilico', 'sin', 'sidral'] + entretenimiento + limpieza_detergentes_t + cuidado_personal_belleza_t

ropa_zapatos_accesorios_t = ropa_zapatos_accesorios
ropa_zapatos_accesorios_b = ['farmacia', 'perro', 'gato', 'mascota', 'carne', 'alimento', 'panaderia',
                             'tortilla', 'pulga', 'alimento', 'bikini'] + entretenimiento + oficina_papeleria_t


#DEPARTMENTS
farmacia_t = list(set(['farmacia'] + farmacia + mama_bebe_t + medicamentos_t + naturales_t + salud_sexual_t +
                      equipo_botiquin_t+ derma_t + vitaminas_suplementos_t + cuidado_personal_belleza_t))


farmacia_b = ['ferreteria', 'jarceria', 'mascota', 'perro', 'gato', 'antifaz', 'abarrote', 'cafe'] + entretenimiento + mascotas_t + salchichoneria_quesos_gourmet_t


super_t = list(set( super_ + frutas_verduras_t + panaderia_tortilleria_t + botanas_dulces_t + carnes_pescados_t + \
             lacteos_huevo_t + salchichoneria_quesos_gourmet_t + alimentos_congelados_regrigerados_t + jugos_bebidas_t + \
             despensa_t + desechables_t + jugueteria_t + ferreteria_jarceria_t + mascotas_t + hogar_t + \
             limpieza_detergentes_t + entretenimiento_t + computo_electronica_t + autos_motos_llantas_t + \
             deportes_t + oficina_papeleria_t + cerveza_vinos_licores_t + ropa_zapatos_accesorios_t + cuidado_personal_belleza_t))
super_b = farmacia + medicamentos_t



categories_json = {
    "Farmacia": {
        "subcats": [
            {
                "Mamá y bebé": {
                    "subcats": [],
                    "tokens": mama_bebe_t,
                    "banned": mama_bebe_b
                }
            },
            {
                "Medicamentos": {
                    "subcats": [],
                    "tokens": medicamentos_t,
                    "banned": medicamentos_b
                }
            },
            {
                "Cuidado personal y belleza": {
                    "subcats": [],
                    "tokens": cuidado_personal_belleza_t,
                    "banned": cuidado_personal_belleza_b
                }
            },
            {
                "Naturales": {
                    "subcats": [],
                    "tokens": naturales_t,
                    "banned": naturales_b
                }
            },
            {
                "Salud sexual": {
                    "subcats": [],
                    "tokens": salud_sexual_t,
                    "banned": salud_sexual_b
                }
            },
            {
                "Equipo y botiquín": {
                    "subcats": [],
                    "tokens": equipo_botiquin_t,
                    "banned": equipo_botiquin_b
                }
            },
            {
                "Derma": {
                    "subcats": [],
                    "tokens": derma_t,
                    "banned": derma_b
                }
            },
            {
                "Vitaminas y suplementos": {
                    "subcats": [],
                    "tokens": vitaminas_suplementos_t,
                    "banned": vitaminas_suplementos_b
                }
            }
        ],
        "tokens": farmacia_t,
        "banned": farmacia_b
    },

    "Super": {
        "subcats": [
            {
                "Frutas y verduras": {
                    "subcats": [],
                    "tokens": frutas_verduras_t,
                    "banned": frutas_verduras_b
                }
            },
            {
                "Panadería y Tortillería": {
                    "subcats": [],
                    "tokens": panaderia_tortilleria_t,
                    "banned": panaderia_tortilleria_b
                }
            },
            {
                "Botanas y Dulces": {
                    "subcats": [],
                    "tokens": botanas_dulces_t,
                    "banned": botanas_dulces_b
                }
            },
            {
                "Carnes y Pescados": {
                    "subcats": [],
                    "tokens": carnes_pescados_t,
                    "banned": carnes_pescados_b
                }
            },
            {
                "Lácteos y huevo": {
                    "subcats": [],
                    "tokens": lacteos_huevo_t,
                    "banned": lacteos_huevo_b
                }
            },
            {
                "Salchichonería, Quesos y Gourmet": {
                    "subcats": [],
                    "tokens": salchichoneria_quesos_gourmet_t,
                    "banned": salchichoneria_quesos_gourmet_b
                }
            },
            {
                "Despensa": {
                    "subcats": [],
                    "tokens": despensa_t,
                    "banned": despensa_b
                }
            },
            {
                "Alimentos Congelados y Regrigerados": {
                    "subcats": [],
                    "tokens": alimentos_congelados_regrigerados_t,
                    "banned": alimentos_congelados_regrigerados_b
                }
            },
            {
                "Jugos y Bebidas": {
                    "subcats": [],
                    "tokens": jugos_bebidas_t,
                    "banned": jugos_bebidas_b
                }
            },
            {
                "Cerveza, Vinos y Licores": {
                    "subcats": [],
                    "tokens": cerveza_vinos_licores_t,
                    "banned": cerveza_vinos_licores_b
                }
            },
            {
                "Desechables": {
                    "subcats": [],
                    "tokens": desechables_t,
                    "banned": desechables_b
                }
            },
            {
                "Juguetería": {
                    "subcats": [],
                    "tokens": jugueteria_t,
                    "banned": jugueteria_b
                }
            },
            {
                "Ferretería y Jarcería": {
                    "subcats": [],
                    "tokens": ferreteria_jarceria_t,
                    "banned": ferreteria_jarceria_b
                }
            },
            {
                "Mascotas": {
                    "subcats": [],
                    "tokens": mascotas_t,
                    "banned": mascotas_b
                }
            },
            {
                "Hogar": {
                    "subcats": [],
                    "tokens": hogar_t,
                    "banned": hogar_b
                }
            },
            {
                "Limpieza y Detergentes": {
                    "subcats": [],
                    "tokens": limpieza_detergentes_t,
                    "banned": limpieza_detergentes_b
                }
            },
            {
                "Entretenimiento": {
                    "subcats": [],
                    "tokens": entretenimiento_t,
                    "banned": entretenimiento_b
                }
            },
            {
                "Cuidado personal y belleza": {
                    "subcats": [],
                    "tokens": cuidado_personal_belleza_t,
                    "banned": cuidado_personal_belleza_b
                }
            },
            {
                "Cómputo y Electrónica": {
                    "subcats": [],
                    "tokens": computo_electronica_t,
                    "banned": computo_electronica_b
                }
            },
            {
                "Ropa, Zapatos y Accesorios": {
                    "subcats": [],
                    "tokens": ropa_zapatos_accesorios_t,
                    "banned": ropa_zapatos_accesorios_b
                }
            },
            {
                "Autos, Motos y llantas": {
                    "subcats": [],
                    "tokens": autos_motos_llantas_t,
                    "banned": autos_motos_llantas_b
                }
            },
            {
                "Deportes": {
                    "subcats": [],
                    "tokens": deportes_t,
                    "banned": deportes_b
                }
            },
            {
                "Oficina y Papelería": {
                    "subcats": [],
                    "tokens": oficina_papeleria_t,
                    "banned": oficina_papeleria_b
                }
            }
        ],
        "tokens": super_t,
        "banned": super_b
    }
}

def get_categories_related(categories_raw, min_score=90, min_bad_score=80, is_name=False):
    if categories_raw:
        categories_raw = clean(categories_raw, is_name)
    #print(categories_raw)
    if categories_raw:
        match_categories = []
        for name, attrs in categories_json.items():
            not_choices = attrs.get('banned')
            bad_results = process.extractBests(categories_raw, not_choices, scorer=fuzz.partial_token_set_ratio,
                                               score_cutoff=min_bad_score)
            if not bad_results:
                choices = attrs.get('tokens')
                results = process.extractBests(categories_raw, choices, scorer=fuzz.partial_token_set_ratio, score_cutoff=min_score)
                if results:
                    match_categories.append(name)
                result_keys = {result[0] for result in results}

                for cat in attrs.get('subcats'):
                    choices = set(list(cat.values())[0].get('tokens'))

                    if (choices & result_keys):
                        not_choices = list(cat.values())[0].get('banned')
                        bad_results_sub = process.extractBests(categories_raw, not_choices,
                                                           scorer=fuzz.partial_token_set_ratio,
                                                           score_cutoff=min_bad_score)
                        if not bad_results_sub:
                            cat_name = list(cat.keys())[0]
                            print("++++++ \t", cat_name, ': ', results)
                            match_categories.append(cat_name)
                        else:
                            cat_name = list(cat.keys())[0]
                            print("------ \t", cat_name, ': ', bad_results_sub)
            else:
                print("------ \t", name, ': ', bad_results)

        aux = {"Limpieza y Detergentes", "Mascotas", "Autos, Motos y llantas", "Hogar"} & set(match_categories)
        if len(aux) > 1 and not is_name:
            return []


        return match_categories
    else:
        return []

def create_categories_in_db():
    print("Creating categories in db")
    db = Pygres(
        {
            "SQL_HOST": os.getenv("SQL_HOST"),
            "SQL_PORT": os.getenv("SQL_PORT"),
            "SQL_DB": os.getenv("SQL_DB"),
            "SQL_USER": os.getenv("SQL_USER"),
            "SQL_PASSWORD": os.getenv("SQL_PASSWORD")
        }
    )
    bp_farma = pd.read_sql("select * from category where source='byprice_farma'", db.conn)
    bp_all = pd.read_sql("select id_category from category where source='byprice'", db.conn)
    bp_source = pd.read_sql("select * from source where key='byprice_farma'", db.conn)



    if bp_source.empty or bp_farma.empty or bp_all.empty:
        category = db.model('category', 'id_category')

        if bp_source.empty:
            source = db.model('source', 'key')
            source.key = 'byprice_farma'
            source.name = 'ByPrice Farma'
            source.logo = 'byprice.png'
            source.type = 'retailer'
            source.retailer = 1
            source.hierarchy = 2
            source.save()


        if bp_farma.empty and len(bp_all) < 20:
            for index, row in bp_all.iterrows():
                category.id_category = int(row.id_category)
                category.source = 'byprice_farma'
                category.save()

        for name, attrs in categories_json.items():
            category.name = name
            category.source = 'byprice'
            category.key = to_key(name)
            category.save()
            id_parent = category.last_id
            for subcat in attrs.get('subcats'):
                name_subcat = list(subcat.keys())[0]
                category.name = name_subcat
                category.source = 'byprice'
                category.key = to_key(name_subcat)
                category.id_parent = id_parent
                category.save()
        db.close()

def get_id_categories():
    print("Getting id categories")
    db = Pygres(
        {
            "SQL_HOST": os.getenv("SQL_HOST"),
            "SQL_PORT": os.getenv("SQL_PORT"),
            "SQL_DB": os.getenv("SQL_DB"),
            "SQL_USER": os.getenv("SQL_USER"),
            "SQL_PASSWORD": os.getenv("SQL_PASSWORD")
        }
    )
    df = pd.read_sql("""SELECT id_category, name as category_name FROM  category where source='byprice'""", db.conn)
    db.close()
    categories = {}
    for index, row in df.iterrows():
        categories[row.category_name] = row.id_category

    return categories
