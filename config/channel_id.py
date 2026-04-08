from collections.abc import Iterable


# Fonte unica de verdade para os canais e seus respectivos nichos.
# Para adicionar um novo canal, basta incluir um novo item nesta lista.
CHANNEL_DEFINITIONS = [
    {"channel_id": "UCGfrC6R2PSMeXv4hdhONB6Q", "niches": ("Fitness",)},
    {"channel_id": "UCEI44xNfQmAukxMf1kW8d5g", "niches": ("Fitness",)},
    {"channel_id": "UCd4g7oX49jtbX7rN-wclkJg", "niches": ("Fitness",)},
    {"channel_id": "UCmK5h2-a4CquS4nIxDN6j7g", "niches": ("Fitness",)},
    {"channel_id": "UCU_q46MCMEu5l2QdshV0hGQ", "niches": ("Fitness",)},
    {"channel_id": "UCPlemwX82_QEWRDC6yYnOCg", "niches": ("Fitness",)},
    {"channel_id": "UC6gJAnIhDn2Fy_jZfICmgSA", "niches": ("Fitness",)},
    {"channel_id": "UCNEUXTXD8IhDqxR7sosYOow", "niches": ("Fitness",)},
    {"channel_id": "UCy-R3H4s0z1HubB_uSiGsHA", "niches": ("Fitness",)},
    {"channel_id": "UC0EPH87G38158HBGzDSSZ1Q", "niches": ("Fitness",)},
    {"channel_id": "UCjbknPNDsL9fd860cDInPGw", "niches": ("Fitness",)},
    {"channel_id": "UCBtOMJf4ZB6yXZQx98vIN7w", "niches": ("Fitness",)},
    {"channel_id": "UCOOLLUHhjJ-I6dXTQ-5cq5g", "niches": ("Fitness",)},
    {"channel_id": "UC7RaQvO8fqoyd62RmIg_yGA", "niches": ("Fitness",)},
    {"channel_id": "UCA0eYfmteLgN7bFZaMDXGUw", "niches": ("Fitness",)},
    {"channel_id": "UCdRhxTB5X5XoJvamINrrijQ", "niches": ("Fitness",)},
    {"channel_id": "UCTmAy9D7NR8sHdn3Oup1wlQ", "niches": ("Fitness",)},
    {"channel_id": "UCaZLOsun_sY6BonjwR8Zo4A", "niches": ("Fitness",)},
    {"channel_id": "UC5BW1WJ3ioahNzFUBdJ7cLQ", "niches": ("Fitness",)},
    {"channel_id": "UCfvg8A2uKlOmC1xWnUk7tpQ", "niches": ("Fitness",)},
    {"channel_id": "UCbgmJsnqdzu9aHTEvQGupfg", "niches": ("Fitness",)},
    {"channel_id": "UCUOsr03iLj627hJm55cmIPw", "niches": ("Fitness",)},
    {"channel_id": "UCLfCo17TCjx7qf-JMhQioLQ", "niches": ("Fitness",)},
    {"channel_id": "UCPX0gLduKAfgr-HJENa7CFw", "niches": ("Fitness",)},
    {"channel_id": "UC3dM3SoKkFJI5WOfY9yOlcw", "niches": ("Fitness",)},
    {"channel_id": "UCA_WXRDt9nz8exYNLto1gSA", "niches": ("Fitness",)},
    {"channel_id": "UCW8w8qOEKr_f1N6fEF13vxQ", "niches": ("Fitness", "Podcast")},
    {"channel_id": "UCC4PiXBt_sGA7r3_eUPXAWg", "niches": ("Fitness",)},
    {"channel_id": "UC5UTV_AyFEzql9uZXMzCI5A", "niches": ("Fitness",)},
    {"channel_id": "UCWNp-LAUL3dQTOaADjTPAdg", "niches": ("Fitness",)},
    {"channel_id": "UC4sQ3Xv9UiSEhkgdTNGK_rA", "niches": ("Fitness",)},
    {"channel_id": "UC9cz05xObaFpB8U72t73IFA", "niches": ("Fitness",)},
    {"channel_id": "UC3wVslvR1r5lKPeh7wpoG8Q", "niches": ("Fitness",)},
    {"channel_id": "UCwf-NgPwT6kfljUyaujLAPw", "niches": ("Fitness",)},
    {"channel_id": "UC8XVVspM8VIBh_DCeul0Qcg", "niches": ("Fitness",)},
    {"channel_id": "UCqQWafqJqrCyxOFPrSlxL_w", "niches": ("Fitness",)},
    {"channel_id": "UCT4nDeU5pv1XIGySbSK-GgA", "niches": ("Financas",)},
    {"channel_id": "UCCE-jo1GvBJqyj1b287h7jA", "niches": ("Financas",)},
    {"channel_id": "UC8mDF5mWNGE-Kpfcvnn0bUg", "niches": ("Financas",)},
    {"channel_id": "UCOjXqrOxAdXa04obIQREfCA", "niches": ("Financas",)},
    {"channel_id": "UC6xhawZf2I30z9kAgqVEIvg", "niches": ("Financas", "Podcast")},
    {"channel_id": "UCM3vJxmuJJkk1r0yzFI9eZg", "niches": ("Financas",)},
    {"channel_id": "UC8oofAsuieQv3imZGvaUDOQ", "niches": ("Financas",)},
    {"channel_id": "UC-hA65Fjv5X8h-J92zc_L8Q", "niches": ("Financas",)},
    {"channel_id": "UCIeL1JF5Q7ALE1qOGPJBm5w", "niches": ("Financas",)},
    {"channel_id": "UCfMA8s_QXPPcuOTjTDFb4vA", "niches": ("Financas", "Podcast")},
    {"channel_id": "UC3Z2XdKUu21_KtMsohZedOQ", "niches": ("Financas",)},
    {"channel_id": "UCPUb-aMI0HiBjC1rsuRN4ng", "niches": ("Financas",)},
    {"channel_id": "UChBtPExX9RjCdmpAizK7ccQ", "niches": ("Financas",)},
    {"channel_id": "UCNGqYuEd86K7dY70jE6dhKg", "niches": ("Financas",)},
    {"channel_id": "UCA8l61FQjT1sGr28L-sbKqA", "niches": ("Financas",)},
    {"channel_id": "UCwONybZQdULFV_ts2911Rqg", "niches": ("Financas",)},
    {"channel_id": "UCrp0zmecZ3TNV8FSR-tjv7A", "niches": ("Financas",)},
    {"channel_id": "UC_mSfchV-fgpPy-vuwML8_A", "niches": ("Financas",)},
    {"channel_id": "UCzJPdSTGj7KPZLjaOatWS4A", "niches": ("Financas", "Podcast")},
    {"channel_id": "UCnBNoed6NaPlpL8blj1yy8Q", "niches": ("Financas",)},
    {"channel_id": "UC6pkw1C4ha1cmU9aDJ8OxUw", "niches": ("Financas",)},
    {"channel_id": "UCj3jEZuLmXOndQJFkPTp3Nw", "niches": ("Financas",)},
    {"channel_id": "UCIjDZzIWu0wlkDbBImhCS7Q", "niches": ("Financas",)},
    {"channel_id": "UCiHUp5cRkjcItPdldyJgd4Q", "niches": ("Financas",)},
    {"channel_id": "UCU_UT4dJbSSPfTK52Yz9Agw", "niches": ("Financas",)},
    {"channel_id": "UC7BsIosUdCz1utgm1Df1y_A", "niches": ("Financas",)},
    {"channel_id": "UCteJrv6W7MWB5SoRvfjF36Q", "niches": ("Financas",)},
    {"channel_id": "UCSXdP8V4jRaw-8YMTpa6glw", "niches": ("Financas",)},
    {"channel_id": "UCLJkh3QjHsLtK0LZFd28oGg", "niches": ("Financas",)},
    {"channel_id": "UC6NgVjfmatbvRuTqWZoegqw", "niches": ("Financas",)},
    {"channel_id": "UCj9R9rOhl81fhnKxBpwJ-yw", "niches": ("Podcast",)},
    {"channel_id": "UC4ncvgh5hFr5O83MH7-jRJg", "niches": ("Podcast",)},
    {"channel_id": "UCWZoPPW7u2I4gZfhJBZ6NqQ", "niches": ("Podcast",)},
    {"channel_id": "UCuUUWPHIioyfmctnDpwsFzw", "niches": ("Podcast",)},
    {"channel_id": "UC9djCiv4e85Kd8G--GWlcoQ", "niches": ("Podcast",)},
    {"channel_id": "UCxcDCeShqZWIUQuEo2iJSVA", "niches": ("Podcast",)},
    {"channel_id": "UC9LH3xFOJCCp2VFRcZjNdRQ", "niches": ("Podcast",)},
    {"channel_id": "UCDE-BCkjudUvQEMrm0XsHUg", "niches": ("Podcast",)},
    {"channel_id": "UC5-aueB1RqpUc-EeAjtx9Lw", "niches": ("Podcast",)},
    {"channel_id": "UCTBhsXf_XRxk8w4rMj6WBOA", "niches": ("Podcast",)},
    {"channel_id": "UCSICdgxW9BIvsHqmSLNU8hQ", "niches": ("Podcast",)},
    {"channel_id": "UCPHvx0NxJubMU8QqZ6X_Efw", "niches": ("Podcast",)},
    {"channel_id": "UCUZfhX79dQrJvAgYefiXCCA", "niches": ("Podcast",)},
    {"channel_id": "UCnlR7Byv1irGXGokMXf9nAQ", "niches": ("Podcast",)},
    {"channel_id": "UC3BM4LVGTrG5OER815AM7Bg", "niches": ("Podcast",)},
    {"channel_id": "UC6iiSl-DJQF5t_YttyUZX6Q", "niches": ("Podcast",)},
    {"channel_id": "UCbroBIg8zvIH8-F4631wJhA", "niches": ("Podcast",)},
    {"channel_id": "UCvm40-L5lbzlxQSbVnfX8pQ", "niches": ("Podcast",)},
    {"channel_id": "UCeL1a4rpEA8UG9IQIewPccg", "niches": ("Podcast",)},
    {"channel_id": "UCLPXpKv6WDc7mtapsbMyPHg", "niches": ("Viagens",)},
    {"channel_id": "UCOHptmhkQl0nPAPpghvJjRA", "niches": ("Viagens",)},
    {"channel_id": "UC5sMjGaQJXkQdZTzGkSjYDg", "niches": ("Viagens",)},
    {"channel_id": "UCWfRNn9fRe5gMc3QaE3AXkA", "niches": ("Viagens",)},
    {"channel_id": "UC-6_6nA-v9W057U5sX3tTXg", "niches": ("Viagens",)},
    {"channel_id": "UCOJNw9aHGRkYuIOqwU7yK-Q", "niches": ("Viagens",)},
    {"channel_id": "UCk6RUGsfWI-3kPrDlMUfvfQ", "niches": ("Viagens",)},
    {"channel_id": "UC6xoasG-FY3P1YHo8_gBG9g", "niches": ("Viagens",)},
    {"channel_id": "UCULwpDVKV_vOZQ4zRyoYgUA", "niches": ("Viagens",)},
    {"channel_id": "UCJy-lSzu5UzVvLn9nXvWovg", "niches": ("Viagens",)},
    {"channel_id": "UChT_BMoR8j1JakGNMCQOAkw", "niches": ("Viagens",)},
    {"channel_id": "UC2HcaGYdio1RRuu3lh9_mHA", "niches": ("Viagens",)},
    {"channel_id": "UC3W3fZG5oGQGGNoBYWwCrTg", "niches": ("Viagens",)},
    {"channel_id": "UCGQJv1EsV4kcVkZJ4FoeWdA", "niches": ("Viagens",)},
    {"channel_id": "UCbVz3Ux2FXbPgrDbOanPLlg", "niches": ("Viagens",)},
    {"channel_id": "UCNU8ZfLKcoJXRspbizt1Ycw", "niches": ("Viagens",)},
    {"channel_id": "UC3UnmT5IgPKdXkvPhdbulaQ", "niches": ("Viagens",)},
    {"channel_id": "UCV6Lb37EQLdaDu0VyI7yDPg", "niches": ("Viagens",)},
    {"channel_id": "UCngY-0fPeVYo-TdIBmj-hGg", "niches": ("Viagens",)},
    {"channel_id": "UCnqaJ5tlyhfQiaQR1id_DBA", "niches": ("Viagens",)},
    {"channel_id": "UCd76luTPgzdZ5FUDk74RK9w", "niches": ("Viagens",)},
]


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _build_channel_ids_by_niche() -> dict[str, list[str]]:
    channels_by_niche: dict[str, list[str]] = {}

    for channel in CHANNEL_DEFINITIONS:
        channel_id = channel["channel_id"]
        for niche in _dedupe_preserve_order(channel["niches"]):
            channels_by_niche.setdefault(niche, []).append(channel_id)

    return channels_by_niche


CHANNEL_IDS_BY_NICHE = _build_channel_ids_by_niche()
CHANNELS_IDS = _dedupe_preserve_order(channel["channel_id"] for channel in CHANNEL_DEFINITIONS)

# Compatibilidade com o código atual.
FITNESS_CHANNELS_IDS = CHANNEL_IDS_BY_NICHE["Fitness"]
FINANCAS_CHANNEL_ID = CHANNEL_IDS_BY_NICHE["Financas"]
PODCAST_CHANNEL_ID = CHANNEL_IDS_BY_NICHE["Podcast"]
VIAGENS_CHANNEL_ID = CHANNEL_IDS_BY_NICHE["Viagens"]


def get_channel_ids_by_niche(niche: str) -> list[str]:
    return CHANNEL_IDS_BY_NICHE.get(niche, [])
