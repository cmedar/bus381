"""
Stop sequence for route 381 (internal id 184), direction 0 (toward Clabucet).
Extracted from Bucharest GTFS — update if STB publishes a new GTFS feed.
Each tuple: (stop_sequence, stop_id, stop_name, lat, lon)
"""
STOPS_DIR0 = [
    (0,  '3665',  'Piata Resita',                  44.379807, 26.099216),
    (1,  '3778',  'Straja',                         44.384170, 26.101183),
    (2,  '3851',  'Izvorul Rece',                   44.388350, 26.105467),
    (3,  '3777',  'Bd. Constantin Brancoveanu',     44.387560, 26.109257),
    (4,  '3779',  'Bd. Alexandru Obregia',          44.388620, 26.116660),
    (5,  '3829',  'Piata Sudului',                  44.392563, 26.120800),
    (6,  '3668',  'Soseaua Oltenitei',              44.394955, 26.120995),
    (7,  '3671',  'Pridvorului',                    44.399980, 26.121134),
    (8,  '3854',  'Costache Stamate',               44.403970, 26.121288),
    (9,  '3924',  'Bd. Tineretului',                44.407997, 26.120237),
    (10, '3824',  'Palatul National al Copiilor',   44.408504, 26.116032),
    (11, '3669',  'Parcul Tineretului',             44.409690, 26.112226),
    (12, '3688',  'Visana',                         44.410760, 26.110056),
    (13, '3782',  'Colegiul National Gh. Sincai',   44.415115, 26.104584),
    (14, '3678',  'Bd. Marasesti',                  44.420060, 26.104223),
    (15, '7257',  'Piata Sf. Gheorghe',             44.432700, 26.103636),
    (16, '7256',  'Universitate',                   44.434372, 26.103134),
    (17, '12353', 'Bd. Nicolae Balcescu',           44.438930, 26.100842),
    (18, '12354', 'Arthur Verona',                  44.442543, 26.099022),
    (19, '6588',  'Piata Romana',                   44.445347, 26.097654),
    (20, '6424',  'Piata Romana',                   44.447220, 26.097073),
    (21, '6599',  'Povernei',                       44.449127, 26.092724),
    (22, '6598',  'Piata Victoriei',                44.451550, 26.087814),
    (23, '6849',  'Muzeul Taranului Roman',         44.453804, 26.082785),
    (24, '6209',  'Bd. Banu Manta',                 44.455498, 26.080873),
    (25, '7057',  'Piata Ion Mihalache',            44.458847, 26.077007),
    (26, '7056',  'Bd. Maresal Averescu',           44.462670, 26.072128),
    (27, '7055',  'Piata Domenii',                  44.465390, 26.067518),
    (28, '6156',  'Sandu Aldea',                    44.467728, 26.061770),
    (29, '6846',  'Aviator Popisteanu',             44.469290, 26.057974),
    (30, '7360',  'Clabucet',                       44.471317, 26.051231),
]


def nearest_stop(lat: float, lon: float) -> tuple[int, str, str]:
    """Return (stop_sequence, stop_id, stop_name) of the closest stop."""
    best = min(
        STOPS_DIR0,
        key=lambda s: (s[3] - lat) ** 2 + (s[4] - lon) ** 2,
    )
    return best[0], best[1], best[2]
