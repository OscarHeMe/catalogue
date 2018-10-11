import os
import pandas as pd
import re
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
                    'musica', 'video', 'fotografia', 'camara', 'libro', 'television', 'radio', 'literatura', 'cuento',
                    'revista']

farmacia = ['cardiobascular', 'bacteria', 'infeccion', 'antiseptico', 'moreton', 'cortadura', 'inmuno',
            'hormonal', 'farmaco', 'hipertension', 'hospital', 'pediatra', 'doctor', 'medicamento']


cuidado_personal_belleza = ['belleza', 'maquillaje', 'rubor', 'mascarilla', 'pies',
                    'repelente', 'cuerpo', 'diente', 'boca', 'bucal', 'higiene', 'tinte',
                    'desodorante', 'perfume', 'cosmetico', 'arruga', 'acne', 'faja' ,
                    'depilar', 'afeitar', 'cabello', 'capilar', 'shampoo', 'acondicionador',
                    'esencia', 'locion', 'fragancia', 'repelente', 'champu', 'caspa',
                    'cepillo', 'alicata', 'cortauna', 'lima', 'labio', 'talco', 'fragancia',
                    'pestana', 'antitranspirante', 'garnier', 'nivea', 'pantene',
                    'gillete', 'revlon', 'esmalte', 'fijador', 'moldeador']

ropa_zapatos_accesorios = ['ropa', 'zapato', 'calzado', 'reloj', 'joyeria' 'pulsera', 'anillo', 'lente',
                         'vesitdo', 'gorra', 'chamarra', 'playera', 'sueter', 'sudadera', 'falda', 'tennis',
                         'pantalon', 'short', 'bermuda', 'pantufla', 'jeans', 'camisa', 'sombrero', 'jersey',
                         'brasier', 'calzon', 'calcetin', 'collar', 'moda', 'maleta']

super_ = ['super', 'hamburguesa', 'hot-dog', 'alimento', 'pizza', 'fiesta', 'pastel', 'cafe']

cerveza_vinos_licores = ['cerveza', 'vino', 'whisky', 'licor', 'alcohol', 'tequila', 'champagne', 'vodka',
                         'mezcal', 'brandy', 'cognac', 'hielo', 'destilado', 'sidra', 'clericot', 'aguardiente']

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
naturales_b = ['yogurt', 'yugur', 'alpura', 'lala', 'helado', 'agua', 'nieve'] + entretenimiento


salud_sexual_t = ['tampon', 'intima', 'sexual', 'condon', 'preservativo', 'embarazo', 'vagina', 'vibrador',
                  'lubricante', 'estimulante', 'anticonceptivo', 'toalla', 'viagra', 'cialis', 'erectil']
salud_sexual_b = ['bebe', 'bano', 'alberca', 'cuerpo', 'cara', 'aceite', 'papel', 'ojos', 'nariz', 'nasal', 'oftal',
                  'laxante', 'motor', 'antitranspirante', 'gel', 'servilleta', 'bebe', 'baby', 'infantil', 'kids', 'bano',
                  'panal', 'lactea', 'leche', 'facial'] + entretenimiento


equipo_botiquin_t = ['equipo medico', 'oximetro', 'baumanometro', 'termometro', 'botiquin', 'jeringa', 'nebulizador', 'alcohol', 'anticeptico',
                     'gasa', 'venda', 'vendita', 'bandita', 'algodon', 'hisopo', 'curita', 'oxigenada', 'suero',
                     'aparato', 'guante latex']
equipo_botiquin_b = [] + entretenimiento + ropa_zapatos_accesorios


derma_t = ['piel', 'cara', 'bloqueador', 'derma', 'acne']
derma_b = ['lacteo', 'huevo', 'leche', 'bebida', 'alimento', 'abarrote', 'despensa', 'pestana', 'shampo',
           'mascara', 'disfraz', 'fiesta', 'papeleria', 'oficina', 'desbloqueado', 'caramelo'] + entretenimiento


vitaminas_suplementos_t = ['vitamina', 'vitaminico', 'suplemento']
vitaminas_suplementos_b = ['antihistaminico'] + entretenimiento


#Ambos
# Ambos
cuidado_personal_belleza_t = derma_t + cuidado_personal_belleza
cuidado_personal_belleza_b = ['lacteo', 'huevo', 'leche', 'disfraz', 'perro', 'mascota', 'bebida', 'alimento',
                              'abarrote', 'despensa', 'caramelo', 'mezcal', 'moda', 'disfraz', 'fiesta', 'papeleria',
                              'oficina', 'disfraces', 'papeleria'] + entretenimiento


# Super
frutas_verduras_t = ['fruta', 'verdura', 'ensalada', 'lechuga', 'manojo', 'tuberculo']
frutas_verduras_b = ['farmacia', 'preparado', 'lata', 'aderezo', 'gelatina', 'congelado', 'bebida', 'frita',
                     'botana', 'jugo', 'fritura', 'nectar', 'cereal', 'palomita', 'papilla', 'mascota', 'polvo'
                     'bebe', 'lacteo', 'detergente', 'perfume', 'aromatizante', 'jabon', 'shampoo', 'cafe', 'leche'
                     ] + entretenimiento


panaderia_tortilleria_t = ['panaderia', 'dona', 'tortilla', 'maiz', 'harina', 'rosca', 'panque', 'bolillo', 'muffin', 'bimbo']
panaderia_tortilleria_b = ['farmacia', 'pantalla', 'pantalon', 'ropa', 'lata', 'frijol', 'frasco', 'arroz', 'gelatina',
                           'jarabe', 'aceite', 'liquido', 'bebida', 'helado', 'nieve'
                           ] + entretenimiento


botanas_dulces_t = ['dulce', 'botana', 'nacho', 'dip', 'chicle', 'paleta', 'chocolate', 'papas', 'fritas',
                    'cacahuate', 'sabritas', 'doritos', 'cheetos', 'palomita', 'caramelo', 'goma mascar', 'chamoy',
                    'salsa valentina', 'salsa maggy', 'salsa inglesa']
botanas_dulces_b = ['farmacia', 'verdura', 'olor', 'color', 'desodorante', 'aromatizante', 'frida', 'cafe',
                    'pasta'
                    ] + entretenimiento + cuidado_personal_belleza


carnes_pescados_t = ['carne', 'ternera', 'puerco', 'cerdo', 'pulpo', 'pescado', 'camaron', 'pulpo', 'salmon', 'calamar']
carnes_pescados_b = ['farmacia', 'lata', 'salsa', 'mascota', 'perro', 'gato', 'gerber', 'papilla', 'despensa', 'abarrote',
                     'jamon', 'salchicha', 'huevo', 'peperoni', 'salami', 'queso', 'gerber', 'nugget',
                     'congelado', 'mortadela', 'cereal', 'papa', 'chicharron', 'botana'
                     ] + entretenimiento


lacteos_huevo_t = ['leche', 'huevo', 'yogurt', 'yoghurt', 'yugur', 'lacteo', 'helado', 'flan', 'natilla']
lacteos_huevo_b = [
                    'farmacia', 'higiene', 'cuerpo', 'mano', 'dental', 'oral', 'carne', 'verdura', 'nugget', 'pollo',
                    'papa', 'pasta', 'mezcla', 'gelatina', 'jugo', 'tostado', 'infantil', 'formula','pan',
                    'tortilla', 'gelatina', 'verdura', 'salsa', 'carne', 'pollo', 'papa', 'pechuga',
                    'botana', 'pizza', 'extractor', 'hamburguesa', 'pescado', 'botana', 'atun', 'caviar'
                   ] + entretenimiento


salchichoneria_quesos_gourmet_t = ['salchicha', 'jamon', 'queso', 'gourmet', 'pavo', 'platillo', 'salami', 'mortadela',
                                   'peperoni', 'chorizo', 'sushi', 'tocino', 'pate']
salchichoneria_quesos_gourmet_b = ['farmacia', 'gerber', 'mascota', 'gato', 'perro', 'cafe', 'bebida', 'yogur',
                                   'chicharron', 'miel', 'verdura', 'leche', 'enlatado', 'crema'] + entretenimiento


alimentos_congelados_refrigerados_t = ['congelado', 'refrigerado', 'gelatina', 'flan', 'natilla', 'hielo', 'helado',
                                       'yogur', 'yugur', 'nugget']
alimentos_congelados_refrigerados_b = ['farmacia', 'polvo', 'electro', 'hogar', 'enlatado', 'gerber', 'mascotas'] + entretenimiento


jugos_bebidas_t = ['jugo', 'agua', 'refresco', 'leche', 'yogurt', 'bebida', 'bebible', 'hielo', 'jumex', 'boing',
                   'cocacola', 'pepsi', 'squirt']
jugos_bebidas_b = ['farmacia', 'polvo', 'oxigenada', 'infantil', 'lactea', 'calentador', 'perro', 'gato', 'cachorro',
                   'jabon', 'alimento', 'galleta', 'nerf', 'capsula', 'barra', 'crema', 'tocador', 'tina', 'purificador',
                   'destilado', 'mascota', 'maquina', 'cafetera', 'curacion', 'bolsa'
                   ] + entretenimiento + cerveza_vinos_licores + cuidado_personal_belleza + farmacia


despensa_t = ['miel', 'mermelada', 'avena', 'cafe', 'aceite','atun', 'sopa', 'pasta', 'despensa', 'abarrote',
              'alimento', 'enlatado', 'cereal', 'galleta', 'azucar', 'sazonador', 'chile',
              'salsa', 'semilla', 'mayoneza', 'aderezo', 'mayonesa', 'herdez', 'tuny', 'costena', 'del monte',
              'nestle', 'kellogs', 'gelatina', 'flan', 'postre'
              ] + panaderia_tortilleria_t + lacteos_huevo_t + jugos_bebidas_t
despensa_b = ['diente', 'farmacia', 'cabello', 'cuerpo', 'pescado', 'fresco', 'mascota', 'perro', 'gato', 'deporte',
              'taza', 'vajilla', 'locion', 'perfume'
              ] + entretenimiento


desechables_t = ['clinex', 'plato', 'vaso', 'servilleta', 'desechable', 'cuchillo', 'cuchara', 'tenedor',
                 'cubiertos', 'higienico', 'panuelo', 'popote']
desechables_b = ['metal', 'cuaderno', 'bebe', 'panal', 'perro', 'mascota', 'porcelana', 'acero', 'farmacia', 'jeringa',
                 'vidrio' 'juego', 'madera', 'sexual', 'femenina', 'panti', 'mantel', 'copa', 'maquillaje', 'alimento',
                 'sopa', 'pasta', 'aguja', 'cocina', 'mesa', 'carbon', 'chupon', 'jgo', 'sarten', 'jardin',
                 'intimo', 'molde', 'tfal', 'exprimidor', 'ekco', 'alcohol', 'tupper', 'porta', 'tazon', 'taza', 'parrilla',
                 'entrenador', 'detergente', 'lavanderia', 'cloro', 'suavizante', 'jabon', 'refractario', 'hermetico',
                 'popit', 'bote', 'tabla', 'botella', 'tarro', 'silicon'
                 ] + entretenimiento


jugueteria_t = ['juguete', 'juego', 'muneco', 'pelota', 'burbuja', 'peluche']
jugueteria_b = ['farmacia', 'bano', 'cubiertos', 'ferreteria', 'desechable', 'cocina', 'pedigree', 'purina'
                'kong', 'cuchillo', 'platos', 'utensilios', 'ropa', 'cosola',
                'sexual', 'herramientas', 'limpieza', 'ps4', 'ps3', 'xbox', '3ds', 'nintendo switch', 'wii', 'alcohol',
                'videojuego'
                ] + entretenimiento


ferreteria_jarceria_t = ['ferreteria', 'jarceria', 'herramienta', 'truper', 'martillo', 'taladro', 'bombilla', 'foco',
                         'cerradura', 'resistol', 'pegamento', 'adhesivo', 'pintura', 'escoba', 'recogedor', 'manguera',
                         'multicontacto', 'insecticida'
                         ]
ferreteria_jarceria_b = ['farmacia', 'deporte', 'perro', 'gato', 'ropa', 'balon', 'electrodomestico', 'aceite', 'llanta'
                         'papeleria', 'acuarela', 'playa', 'balon', 'parabrisas', 'juguete', 'playera',
                         'camiseta', 'maleta', 'alberca', 'neumatico'
                         ] + entretenimiento


mascotas_t = ['mascota', 'perro', 'peces', 'gato', 'pajaro', 'tortuga', 'cachorro', 'antipulga', 'morder', 'carnaza',
              'hamster', 'electronic']
mascotas_b = ['farmacia', 'pegamento', 'adhesivo', 'cubiertos', 'chocolate', 'desechable', 'vaso',
              'vajilla', 'abarrotes', 'nino', 'flor', 'papel', 'servilleta', 'cuchara', 'tenedor',
              'herramienta', 'truper', 'martillo', 'taladro', 'bombilla', 'foco',
              'cerradura', 'resistol', 'pegamento', 'adhesivo', 'pintura', 'escoba', 'recogedor', 'manguera',
              'cuchillo', 'sellador', 'pila', 'navidad', 'evento' 'electro', 'alcohol', 'vino', 'porta', 'fiesta',
              'detergente', 'kleenex', 'panuelo', 'papel', 'escalera', 'funda', 'cocina', 'higienieco', 'cerveza', 'licor',
              'copa', 'despensa', 'hielera', 'palillo', 'botana', 'bebida', 'pila', 'candado', 'locion', 'fragancia',
              'tabla', 'carbon', 'parrilla', 'regalo', 'jardin', 'llanta', 'neumatico', 'congelado', 'despensa', 'aceite',
              'moto', 'carro', 'aromatizante', 'multicontacto', 'evento', 'restaurador', 'pulir', 'calzado', 'insecticida',
              'taza', 'direccion', 'hidraulica', 'limpiador', 'aromatizante', 'toalla', 'toallita', 'blanqueador', 'cloro',
              'brasso', 'fibra', 'hermetico', 'tupper', 'gancho', 'vidrio', 'destenidos', 'boligrafo', 'vanish', 'basura',
              'musculo', 'destapacano', 'colorante', 'jabonzote', 'multiusos', 'sandwichera', 'aromatizante', 'ajax',
              'lavanderia', 'zapato', 'calcomania', 'pinol'
              ] + entretenimiento


hogar_t = ['hogar', 'blancos', 'bano', 'comedor', 'sala', 'mueble', 'silla', 'cocina', 'casa', 'domestico', 'cojin',
           'sabana', 'mesa', 'patio', 'adorno', 'plato', 'cubierto', 'cubiertos', 'taza', 'jardin', 'iluminacion',
           'lampara', 'vajilla', 'olla', 'tocador', 'tapete', 'alfombra', 'electrodomestico', 'colchon', 'cobija',
           'refractario'
           ]
hogar_b = ['farmacia', 'mascota', 'perro', 'gato', 'carro', 'auto', 'perfume', 'locion', 'abarrote', 'despensa',
           'desechable', 'verdura', 'fruta', 'alimento', 'bebida', 'juguete', 'detergente', 'bebe', 'panaderia',
           'salchichoneria', 'globo', 'vino', 'alcohol', 'cerveza', 'nino', 'aderezo', 'papel', 'congelado', 'boligrafo',
           'lapiz', 'lapices', 'cuaderno', 'detergente', 'carbon', 'folder', 'tijeras', 'hockey', 'futbolito', 'billar',
           'rompecabezas', 'destreza', 'jabon', 'lavatrastes', 'candado', 'herramienta', 'martillo', 'truper', 'taladro',
           'servilleta', 'cartas', 'bolsa', 'ensalada', 'vela', 'bebible', 'lacteo', 'yogur', 'enlatado', 'farmacia',
           'higiene', 'sexual', 'intimo', 'pingpong', 'estuchera', 'calculadora', 'manguera', 'fertilizante', 'abono',
           'disfraz', 'disfraces', 'globo', 'mascara', 'unicel', 'servilleta', 'higienico', 'marcador', 'cuaderno', 'mochila',
           'papeleria', 'tijeras', 'calculadora', 'libreta', 'ligas', 'insecticida', 'detergente', 'lapiz', 'boligrafo',
           'libreta', 'geometria', 'tijeras', 'marcador', 'boligrafo', 'cuaderno', 'libreta'
           ] + entretenimiento


limpieza_detergentes_t = ['limpieza', 'jabon', 'detergente', 'manchas', 'aromatizante', 'desengrasante', 'escoba',
                          'plumero', 'recogedor', 'trapeador', 'cloro', 'lavatrastes', 'suavizante', 'parabrisas',
                          'glade', 'airwick', 'insecticida', 'limpiador', 'microfibra', 'cloralex', 'atrapapolvo',
                          'vanish', 'brasso', 'destapacano', 'lysol', 'ajax'
                          ]
limpieza_detergentes_b = ['farmacia', 'fertilizante', 'jardin', 'cara', 'cabello', 'manos', 'cuerpo', 'crema', 'auto',
                          'carro', 'cuaderno', 'higienico', 'palillo', 'libreta', 'lapiz', 'tijera', 'frazada', 'papeleria',
                          'lonchera', 'mochila', 'prit', 'lapiz', 'adhesivo', 'candado', 'cristal', 'escolar', 'cocina',
                          'fiesta', 'confeti', 'mascota', 'perro', 'gato', 'crayones', 'globo', 'vaso', 'cubiertos', 'desechable',
                          'pluma', 'boligrafo', 'sarten', 'plato', 'vajilla', 'bebe', 'shampoo', 'candado', 'carpeta',
                          'bombilla', 'foco', 'lampara', 'linterna', 'poster', 'cuaderno', 'cuchara', 'tenedor', 'cuchillo',
                          'jardin', 'olla', 'portafolio', 'documento', 'pintura', 'regalo', 'tinta', 'pastel', 'party', 'cicuitos',
                          'juguete', 'bebe', 'alimento', 'herramienta', 'llave', 'desarmador', 'corporal', 'facial', 'popote',
                          'tarjeta', 'kids', 'tabla', 'scribe', 'parabrisas'
                          ] + entretenimiento


entretenimiento_t = ['arte', 'telescopio', 'baraja', 'casino', 'juegos', 'consola', 'nintendo', 'play station',
                     'xbox', 'hasbro', 'matel', 'muneco', 'platilina'] + entretenimiento
entretenimiento_b = ['farmacia', 'sarten', 'desechable', 'olla', 'herramienta', 'ferreteria', 'jarceria', 'hogar',
                     'electrodomestico', 'alimento', 'bebida', 'peluche', 'cerveza', 'vino', 'alcohol', 'agua', 'lacteos',
                     'jugo', 'sexual', 'consolador']


computo_electronica_t = ['tecnologia', 'impresora', 'electronico', 'celular', 'telefono', 'computo', 'computadora', 'pc',
                         'teclado', 'monitor', 'pantalla', 'bocina', 'cargador', 'usb', 'game', 'estereo', 'laptop',
                         'gps', 'phone', 'huawei', 'sony', 'samsung', 'panasonic', 'microsoft',
                         'software', 'lenovo', 'kingston', 'dell', 'thoshiba', 'intel', 'toshiba', 'bocina',
                         'estereo', 'display', 'mouse', 'inalambrico', 'nvidia']
computo_electronica_b = ['farmacia', 'mickey', 'mascota', 'maskingtape', 'ferreteria', 'jarceria', 'silla',
                         'patio', 'pegamento', 'herramientas', 'adhesivo', 'sellador', 'ropa', 'maleta'
                         ] + entretenimiento


autos_motos_llantas_t = ['moto', 'auto', 'carro', 'llanta', 'parabrisas', 'motocilceta', 'automovil', 'prestone', 'bardahl',
                         'vistony', 'gtx', 'penzzoil', 'quackerstate', 'castrol', 'automobile', 'octanaje', 'gasolina',
                         'neumatico']
autos_motos_llantas_b = ['farmacia', 'autores', 'ferreteria', 'carriola', 'bebe', 'sierra', 'taladro', 'desarmador',
                         'tecnologia', 'bombilla', 'computo', 'inteligente', 'hogar', 'abrillantador',
                         'brillante', 'locomot', 'juguete', 'control', 'remoto', 'pista', 'cocina', 'infantil',
                         'automatizado', 'automatico', 'autobronceado', 'autoasiento', 'nino', 'bebida', 'despensa',
                         'smart', 'phone', 'motorola', 'lego', 'electro', 'muneco', 'hogar', 'natacion', 'balon',
                         'ropa', 'mascota', 'perro', 'peces', 'pajaro', 'tortuga', 'cachorro', 'antipulga', 'morder', 'carnaza',
                         'hamster'
                         ] + entretenimiento + ferreteria_jarceria_t + mascotas_t


deportes_t = ['deporte', 'pesas', 'futbol', 'football', 'natacion', 'baseball', 'beisbol', 'basquetbol',
              'basketball', 'volibol', 'volleyball', 'pelota', 'balon', 'gimnasio', 'caminadora', 'bicicleta',
              'ciclismo', 'raqueta', 'nike', 'adidas', 'goggles', 'alberca']
deportes_b = ['farmacia', 'bano', 'aromatizante', 'hogar', 'cocina', 'foco', 'pintura', 'herramienta', 'jarceria',
              'aceite', 'hogar', 'pintura', 'sierra', 'taladro', 'martillo', 'limpiador',
              'escalera', 'desarmador', 'jardin', 'computo', 'liquido', 'asiento', 'aromatizante',
              'juguete', 'bath', 'lampara', 'linterna', 'hidraulico', 'pulidor', 'limpia', 'parabrisas',
              'bebe', 'bocina', 'detergente', 'jabon', 'polish', 'tinta', 'colorante', 'navaja', 'colchon', 'esponja',
              'mascota', 'perro', 'peces', 'gato', 'pajaro', 'tortuga', 'cachorro', 'antipulga', 'morder', 'carnaza',
              'hamster', 'cepillo', 'pegamento', 'adhesivo', 'carbon', 'cocina', 'naproxeno', 'pila', 'extension', 'juego',
              'casa', 'casita', 'castillo', 'juguete', 'house', 'pastel'
              ] + entretenimiento + autos_motos_llantas_t + mascotas_t


oficina_papeleria_t = ['oficina', 'cuaderno', 'lapiz', 'lapices', 'lapicera', 'gis', 'colores', 'pluma', 'plumon',
                       'pepeleria','papel', 'confeti', 'globo', 'tarjeta', 'serpentina', 'libreta', 'folder', 'unicel',
                       'engrapadora', 'perforadora', 'cartulina', 'monografia', 'boligrafo', 'hoja', 'tijera', 'calculadora',
                       'escolar', 'crayones', 'carpeta', 'calcomania', 'geometria'
                       ]
oficina_papeleria_b = ['farmacia', 'higienico', 'bano', 'labial', 'belleza', 'pestana', 'delineador', 'maquillaje',
                       'ojo', 'labio', 'ganso', 'cojin', 'cobija', 'almohada', 'pavo', 'juguete', 'mascota', 'perro',
                       'gato', 'disfraz', 'disfraces', 'computo', 'tecnologia', 'electro', 'mascara', 'maquillaje', 'plato',
                       'servilleta', 'higien', 'hielera', 'bebida', 'vaso', 'laptop', 'impresora', 'bebe', 'antifaz',
                       'monstruo', 'cubiertos', 'agua', 'hogar', 'alcohol'
                       ] + entretenimiento


cerveza_vinos_licores_t = cerveza_vinos_licores
cerveza_vinos_licores_b = ['farmacia', 'etilico', 'sin', 'sidral'] + entretenimiento + limpieza_detergentes_t + cuidado_personal_belleza_t


ropa_zapatos_accesorios_t = ropa_zapatos_accesorios
ropa_zapatos_accesorios_b = ['farmacia', 'perro', 'gato', 'mascota', 'carne', 'alimento', 'panaderia',
                             'tortilla', 'pulga', 'alimento', 'bikini', 'juguete', 'nenuco'] + entretenimiento + oficina_papeleria_t


#DEPARTMENTS
farmacia_t = list(set(['farmacia'] + farmacia + mama_bebe_t + medicamentos_t + naturales_t + salud_sexual_t +
                      equipo_botiquin_t+ derma_t + vitaminas_suplementos_t + cuidado_personal_belleza_t))

farmacia_b = ['ferreteria', 'jarceria', 'mascota', 'perro', 'gato', 'antifaz', 'abarrote', 'cafe', 'moda', 'disfraz',
              'fiesta', 'papeleria', 'oficina', 'phone', 'caramelo', 'despensa', 'alpura', 'lala', 'yogurt', 'helado',
              'nieve', 'abarrote'] + \
             entretenimiento + mascotas_t + salchichoneria_quesos_gourmet_t + cerveza_vinos_licores


super_t = list(set( super_ + frutas_verduras_t + panaderia_tortilleria_t + botanas_dulces_t + carnes_pescados_t + \
             lacteos_huevo_t + salchichoneria_quesos_gourmet_t + alimentos_congelados_refrigerados_t + jugos_bebidas_t + \
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
                "Alimentos Congelados y Refrigerados": {
                    "subcats": [],
                    "tokens": alimentos_congelados_refrigerados_t,
                    "banned": alimentos_congelados_refrigerados_b
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
                "Ferretería y Jarciería": {
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

def get_categories_related(categories_raw, min_score=90, min_bad_score=80, is_name=False, names=None):
    if categories_raw:
        categories_raw = clean(categories_raw, is_name)
    if names:
        names = clean(names, True)
    #print(categories_raw)
    if categories_raw:
        match_categories = []
        for name, attrs in categories_json.items():
            not_choices = attrs.get('banned')
            bad_results = process.extractBests(categories_raw, not_choices, scorer=fuzz.partial_token_set_ratio,
                                               score_cutoff=min_bad_score)
            if names:
                bad_results += process.extractBests(names, not_choices, scorer=fuzz.partial_token_set_ratio,
                                                   score_cutoff=min_bad_score)
            if not bad_results:
                choices = attrs.get('tokens')
                results = process.extractBests(categories_raw, choices, scorer=fuzz.partial_token_set_ratio, score_cutoff=min_score)
                if results:
                    # print("++++++ \t", name, ': ', results)
                    match_categories.append(name)
                result_keys = {result[0] for result in results}

                for cat in attrs.get('subcats'):
                    choices = set(list(cat.values())[0].get('tokens'))

                    if (choices & result_keys):
                        not_choices = list(cat.values())[0].get('banned')
                        bad_results_sub = process.extractBests(categories_raw, not_choices,
                                                           scorer=fuzz.partial_token_set_ratio,
                                                           score_cutoff=min_bad_score)
                        if names:
                            bad_results_sub += process.extractBests(names, not_choices,
                                                                   scorer=fuzz.partial_token_set_ratio,
                                                                   score_cutoff=min_bad_score)
                        if not bad_results_sub:
                            cat_name = list(cat.keys())[0]
                            match_categories.append(cat_name)
                            # print("++++++ \t", cat_name, ': ', results)
            #             else:
            #                 cat_name = list(cat.keys())[0]
            #                 print("------ \t", cat_name, ': ', bad_results_sub)
            # else:
            #     print("------ \t", name, ': ', bad_results)
        if {"Mascotas", "Autos, Motos y llantas"} & set(match_categories):
            aux = set(match_categories) - {"Super"}
            if len(aux) > 1:
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
