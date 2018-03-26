'''
Item attributes normalizing functions depending on every retailer format
'''
import re
import unicodedata
import json
import os.path
from . import normalize_text

# Lista de las claves de nombres de atributos
attr_keys=[
    'active',
    'measure_units',
    'measure_value',
    'dose',
    'presentation',
    'quantity',
    'ingredients',
    'size',
    'flavour',
    'color',
    'model',
    'origin',
    'smell',
    'country',
    'type',
    'style'
]

# Lista de atributos que DEBEN tener cantidades numéricas para validar el regex
#re.search(r'\d+(\.\d*)?[ \t\n\r\f\v]*g[ \t\n\r\f\v]+',txt)
#re.search(r'(\d+\.\d*)?[ \t\n\r\f\v]*crema[ \t\n\r\f\v]*',txt)
attr_with_qty=["measure_units",]

# Load attribute map equivalences => Cambiará a una consulta rest a catálogo!!!
if os.path.isfile('app/models/map_attributes.json'):
    with open('app/models/map_attributes.json') as data_file:
        map_attr = json.load(data_file)
    ####### Sustituir lo de arriba por un request al servicio de catálogo para que pase todos los atributos


def exists_attr(attributes, attr_name, t_name, t_val):
    """
        Method that verifies already parsed items
        Returns:  True or False
    """
    attr_with_qty=["measure_units",]
    if attr_name not in attributes.keys():
        return False
    for at in attributes[attr_name]:
        if at[0] == t_name and at[1] == t_val:
            return True
        try:
            if at[0] == t_name and float(at[1]) == float(t_val):
                return True   
        except:
            ## Not parseable to Float
            pass
    return False


def get_from_text(raw_text):
    """ This function gets the attributes from the item.
    For example the measurement unit, the presentation quantity, etc.
    Returns: {'presentation': [('tablet', None), ('unit', 15.0)], 'measure_units': [('mg', 2.0)]}
    """
    attributes = {}

    # normalizamos textos
    text = raw_text.replace('/',' ')
    text =  normalize_text.remove_accents(text)
    text = re.sub('[^A-Za-z0-9 ]+\.', '', text)
    text = text.lower()
    #print(text)

    '''
    Cambiar el loop por match de regexp
    re.search(r'(\d+\.?[\d+]?[\s]?[mg|g|ml|%]+)',txt).group(1)
    '''

    # Iteramos por cada palabra del texto,
    words = text.split()
    for i,word in enumerate(words):
        # Iteramos por los atributos
        for attr_name,elems in map_attr.items():

            # Iteramos por los elementos del tipo de atributo
            for elem in elems:

                if 'match' not in elem:
                    continue

                if word in elem['match']:
                    # Guardamos el atributo
                    if attr_name not in attributes:
                        attributes[attr_name] = []

                    t_name = elem['name']
                    t_val = None
                    # Si la palabra anterior es un número, guardamos como valor de la variable
                    try:
                        t_val = float(words[i-1])
                    except:
                        t_val = None
                    # Guardamos el valor del atributo
                    attributes[attr_name].append((t_name,t_val))
        
    # Iteramos por los atributos
    for attr_name,elems in map_attr.items():
        # Iteramos por los elementos del tipo de atributo
        for elem in elems:

            if 'match' not in elem:
                continue

            for keyword in elem['match']:
                t_name = elem['name']
                t_val = None
                # Patrón de regex dependiendo si debe tener catidad numérica
                if attr_name in attr_with_qty:
                    pattern = re.compile('(\d+(\.*\d*)?)[ \t\n\r\f\v]*'+keyword+'[ \t\n\r\f\v\W]+')
                else:
                    pattern = re.compile('(\d+\.*\d*)?[ \t\n\r\f\v]*'+keyword+'[ \t\n\r\f\v\W]+')
                match = match = re.search(pattern,text)
                if match is not None:
                    # Si no existe el dict, lo creamos
                    if attr_name not in attributes:
                        attributes[attr_name] = []
                    # Vemos si tiene valor el match
                    match_val = match.group(1)
                    if match_val is not None:
                        t_val = match_val
                    # Verificamos si este elemento ya fue parseado anteriormente para no agrgarlo
                    if not exists_attr(attributes, attr_name, t_name, t_val):
                        attributes[attr_name].append((t_name,t_val))

    
    # Si coincide con alguna palabra del mapa obtenemos key => attributo / match => valor
    return attributes
    

'''
#test = "Amaryl   tabletas 15 pzas de 2 mg c/u"
#test = "Always active noche con alas 24 piezas Toallas femeninas"
test = "Bristaflamg crema 60gr Aceclofenaco  1.5g - Almirall"
#pemix  comprimidos 1 mg 25 pzas
#pilovait  1 mg tabletas 28 pzas
#plidan compuesto  comprimidos 10125 mg 20 pzas
print(test)
attrs = get_from_text(test)
print(attrs)
'''
