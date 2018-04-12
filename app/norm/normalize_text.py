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

def tuplify(data):
    _tup = tuple('{}'.format(x) for x in data.split(','))
    if len(_tup) == 1:
        return str(_tup).replace(',','')
    return str(_tup)

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
