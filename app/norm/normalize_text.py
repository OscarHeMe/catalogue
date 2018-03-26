import re
import string
import unicodedata

'''
Normaliza textos a formatos sin acentos o con formato
para keys a base de datos
'''
def key_format(data):
    if not isinstance(data, str):
        return None
    return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.ascii_letters or x == " ").lower().replace(" ","_").replace("_y_","_").replace("_e_","_")

def simple_format(data):
    if not isinstance(data, str):
        return None
    return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.ascii_letters+string.digits or x == " ").lower()

def remove_accents(data):
    if not isinstance(data, str):
        return None
    return ''.join(x for x in unicodedata.normalize('NFKD', data) if x in string.ascii_letters+string.digits+"." or x == " ").lower()

'''
Normaliza el precio, de text a numeric
'''
def price(price_raw):
    # Checar si no es número
    # Si no es número quitamos símbolos
    # Si siguie sin poderse, regresamos none
    if len(str(price_raw)) > 0:
        if ( type(price_raw) is not int ) and ( type(price_raw) is not float ):
            price = price_raw.replace('$', '').replace(',', '')
            try:
                price = float(price)
            except ValueError:
                price = None
        else:
            price = price_raw
    return price


'''
Función que regresa texto normalizado de la
promoción y el precio del item de la promoción
promo = { 'description' : '<text>' , 'price' : <float> }
'''
def promotion(text):

    return promo

def standardize(text):
    # Measurement units
    tablets = 'TAB'
    capsules = 'CAP'
    units = 'U'
    sus = 'SUSPENSION'
    crema = 'CREMA'
    pomada = 'POMADA'
    ampolleta = 'AMPOLLETAS'

    rep_map = [
        [u'Ü', 'U'],
        [u'C/', ''],
        [u'CON', ''],
        [u'-', ' '],
        [u'.', ''],
        [u'  ', ' '],
        [u'/', ' '],
        ['MILIGRAMOS', 'MG'],
        ['MGS', 'MG'],
        ['GRAMOS', 'G'],
        ['GRS', 'G'],
        ['MILILITROS', 'ML'],
        ['LITROS', 'L'],
        ['TABLETAS', tablets],
        ['TABL', tablets],
        ['TABS', tablets],
        [' TB', tablets],
        ['GRAJEAS', tablets],
        ['GRAGEAS', tablets],
        ['COMPRIMIDOS', tablets],
        ['CAP', capsules],
        ['CAPS', capsules],
        ['CAPSULAS', capsules],
        [u'CÁPSULAS', capsules],
        ['PIEZAS', units],
        ['PZ', units],
        ['PZS', units],
        [u'SUSPENSIÓN', sus],
        ['SUSP ', sus + ' '],
        ['AMPOLLETA', ampolleta],
        ['UNGUENTO', crema]
    ]
    re_map = [
        [r'\s+(\d+)([%s|%s|%s|%s|%s|%s|%s])' % (tablets, capsules, units, sus, crema, pomada, sus), r' \1 \2'],
        [r'\s+(\d+)([G|MG|ML|L])', r' \1 \2']
    ]
    text = text.upper()
    for s, rep in rep_map:
        text = text.replace(s, rep)
    for reg, rep in re_map:
        text = re.sub(reg, rep, text)
    text = text.lower()
    return text
