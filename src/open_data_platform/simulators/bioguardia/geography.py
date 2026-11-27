"""Municipalities that host BioGuardia reserves.

Codes and names are IBGE reference data, matched by name and UF against the
IBGE Localidades API on 2026-11-26 (the same source as P1-08). Coordinates are
approximate town locations typed by hand; they place reserves, nothing more.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Municipality:
    code: int
    name: str
    uf: str
    lat: float
    lon: float


MUNICIPALITIES: tuple[Municipality, ...] = (
    Municipality(2925303, "Porto Seguro", "BA", -16.45, -39.06),
    Municipality(2932507, "Una", "BA", -15.29, -39.08),
    Municipality(2914901, "Itacaré", "BA", -14.28, -38.99),
    Municipality(2913606, "Ilhéus", "BA", -14.79, -39.05),
    Municipality(2925501, "Prado", "BA", -17.34, -39.22),
    Municipality(2905602, "Camacan", "BA", -15.42, -39.50),
    Municipality(2927705, "Santa Cruz Cabrália", "BA", -16.28, -39.03),
    Municipality(2933505, "Wenceslau Guimarães", "BA", -13.69, -39.48),
    Municipality(3204609, "Santa Teresa", "ES", -19.94, -40.60),
    Municipality(3203205, "Linhares", "ES", -19.39, -40.07),
    Municipality(3201902, "Domingos Martins", "ES", -20.36, -40.66),
    Municipality(3201605, "Conceição da Barra", "ES", -18.59, -39.73),
    Municipality(3205010, "Sooretama", "ES", -19.19, -40.10),
    Municipality(3201407, "Castelo", "ES", -20.60, -41.18),
    Municipality(3140308, "Marliéria", "MG", -19.71, -42.73),
    Municipality(3103702, "Araponga", "MG", -20.67, -42.52),
    Municipality(3113404, "Caratinga", "MG", -19.79, -42.14),
    Municipality(3138609, "Lima Duarte", "MG", -21.84, -43.79),
    Municipality(3102050, "Alto Caparaó", "MG", -20.43, -41.87),
    Municipality(3303807, "Paraty", "RJ", -23.22, -44.71),
    Municipality(3305802, "Teresópolis", "RJ", -22.41, -42.97),
    Municipality(3303906, "Petrópolis", "RJ", -22.51, -43.18),
    Municipality(3302254, "Itatiaia", "RJ", -22.49, -44.56),
    Municipality(3303401, "Nova Friburgo", "RJ", -22.28, -42.53),
    Municipality(3305604, "Silva Jardim", "RJ", -22.65, -42.39),
    Municipality(3300100, "Angra dos Reis", "RJ", -23.01, -44.32),
    Municipality(3301850, "Guapimirim", "RJ", -22.54, -42.98),
    Municipality(3555406, "Ubatuba", "SP", -23.43, -45.08),
    Municipality(3509908, "Cananéia", "SP", -25.01, -47.93),
    Municipality(3520301, "Iguape", "SP", -24.71, -47.55),
    Municipality(3520400, "Ilhabela", "SP", -23.78, -45.36),
    Municipality(3513603, "Cunha", "SP", -23.07, -44.96),
    Municipality(3550001, "São Luiz do Paraitinga", "SP", -23.22, -45.31),
    Municipality(3521200, "Iporanga", "SP", -24.59, -48.59),
    Municipality(3514809, "Eldorado", "SP", -24.52, -48.11),
    Municipality(3551801, "Sete Barras", "SP", -24.39, -47.93),
    Municipality(3509700, "Campos do Jordão", "SP", -22.74, -45.59),
    Municipality(3506359, "Bertioga", "SP", -23.85, -46.14),
    Municipality(4116208, "Morretes", "PR", -25.48, -48.83),
    Municipality(4109500, "Guaraqueçaba", "PR", -25.31, -48.33),
    Municipality(4101200, "Antonina", "PR", -25.43, -48.71),
    Municipality(4108304, "Foz do Iguaçu", "PR", -25.54, -54.59),
    Municipality(4109609, "Guaratuba", "PR", -25.88, -48.57),
    Municipality(4105300, "Céu Azul", "PR", -25.15, -53.85),
    Municipality(4218905, "Urubici", "SC", -28.01, -49.59),
    Municipality(4215703, "Santo Amaro da Imperatriz", "SC", -27.69, -48.78),
    Municipality(4209102, "Joinville", "SC", -26.30, -48.85),
    Municipality(4202404, "Blumenau", "SC", -26.92, -49.07),
    Municipality(4202503, "Bom Jardim da Serra", "SC", -28.34, -49.62),
    Municipality(4205803, "Garuva", "SC", -26.03, -48.85),
    Municipality(4303608, "Cambará do Sul", "RS", -29.05, -50.15),
    Municipality(4318200, "São Francisco de Paula", "RS", -29.45, -50.58),
    Municipality(4321501, "Torres", "RS", -29.34, -49.73),
    Municipality(4306320, "Derrubadas", "RS", -27.26, -53.86),
    Municipality(4311775, "Maquiné", "RS", -29.68, -50.21),
)

# IBGE state codes: the first two digits of a municipality code.
UF_CODES: dict[str, int] = {
    "BA": 29,
    "ES": 32,
    "MG": 31,
    "RJ": 33,
    "SP": 35,
    "PR": 41,
    "SC": 42,
    "RS": 43,
}

# Brazil's bounding box, used to tell a planted out-of-range coordinate.
BRAZIL_LAT = (-33.8, 5.3)
BRAZIL_LON = (-74.0, -34.7)
