from typing import List, Sequence
import pandas as pd

from .outils_communs import LocalINPNPaths, filter_parquet, ensure_exists


# Constantes ZNIEFF
ESPECES_KEY_COL = "nm_sffzn"
HABITATS_KEY_COL = "NM_SFFZN"

ESPECES_KEEP_COLS = ["nm_sffzn", "cd_ref", "cd_nom", "fg_esp", "groupe_taxo"]
HABITATS_KEEP_COLS = ["NM_SFFZN", "CD_TYPO", "LB_TYPO", "CD_HAB", "LB_CODE", "LB_HAB", "ID_TYPO_INFO"]


def parse_codes_znieff(raw: str) -> List[str]:
    """
    Accepte des codes ZNIEFF séparés par ; ou , retours ligne, tabulations.
    Dé-doublonne en conservant l'ordre.
    Valide que chaque code est composé de 9 chiffres.
    """
    if raw is None:
        return []
    cleaned = raw.replace(",", ";").replace("\n", ";").replace("\t", ";")
    out: List[str] = []
    seen = set()
    for c in (x.strip() for x in cleaned.split(";")):
        if c and c not in seen:
            seen.add(c)
            out.append(c)

    # Validation: chaque code ZNIEFF doit être composé de 9 chiffres
    for c in out:
        if not (c.isdigit() and len(c) == 9):
            raise ValueError(
                f"Code ZNIEFF invalide: '{c}'. Un code ZNIEFF doit être composé de 9 chiffres."
            )

    return out


def load_znieff_info(paths: LocalINPNPaths) -> pd.DataFrame:
    """Charge les informations générales des ZNIEFF."""
    ensure_exists(paths.znieff_infos_generales)

    zn = pd.read_parquet(paths.znieff_infos_generales, columns=["NM_SFFZN", "LB_ZN", "TY_ZONE"])
    zn["NM_SFFZN"] = zn["NM_SFFZN"].astype(str).str.strip()

    zn = zn.rename(columns={
        "NM_SFFZN": "ID ZNIEFF",
        "LB_ZN": "Nom ZNIEFF",
        "TY_ZONE": "Type ZNIEFF",
    })

    # évite les doublons éventuels (on garde le 1er)
    zn = zn.drop_duplicates(subset=["ID ZNIEFF"], keep="first")

    return zn


def export_especes_znieff(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    """Exporte les espèces ZNIEFF filtrées par codes."""
    df = filter_parquet(
        parquet_file=paths.znieff_espece,
        key_col=ESPECES_KEY_COL,
        keep_cols=ESPECES_KEEP_COLS,
        codes=codes,
    )

    if df.empty:
        # Crée un DataFrame vide avec les noms sources puis renomme
        empty = pd.DataFrame(columns=["nm_sffzn", "cd_ref", "cd_nom", "fg_esp", "groupe_taxo", "LB_NOM"])
        return empty.rename(columns={
            "nm_sffzn": "ID ZNIEFF",
            "cd_nom" : "CD_NOM",
            "cd_ref" : "CD_REF",
            "fg_esp": "Type espèce",
            "groupe_taxo": "Groupe taxonomique",
            "LB_NOM": "Nom scientifique",
        })

    # TAXREF: jointure sur cd_nom -> CD_NOM pour récupérer LB_NOM
    ensure_exists(paths.taxref)
    tax = pd.read_parquet(paths.taxref, columns=["CD_NOM", "LB_NOM"])

    df["cd_nom"] = df["cd_nom"].astype(str).str.strip()
    tax["CD_NOM"] = tax["CD_NOM"].astype(str).str.strip()

    df = df.merge(tax, how="left", left_on="cd_nom", right_on="CD_NOM").drop(columns=["CD_NOM"])

    # Créer les colonnes de sortie en majuscules demandées
    df["CD_NOM"] = df["cd_nom"].astype(str).str.strip()
    df["CD_REF"] = df["cd_ref"].astype(str).str.strip()

    # Mapping fg_esp -> libellé
    fg_map = {
        "A": "Autre espèce",
        "E": "Autre espèce à enjeux",
        "D": "Déterminante",
        "C": "Confidentielle",
    }
    df["fg_esp"] = df["fg_esp"].astype(str).str.strip().map(fg_map).fillna(df["fg_esp"])

    # Créer les colonnes finales explicitement à partir des colonnes sources
    df["ID ZNIEFF"] = df["nm_sffzn"].astype(str).str.strip()
    df["Type espèce"] = df["fg_esp"]
    df["Groupe taxonomique"] = df["groupe_taxo"]
    df["Nom scientifique"] = df["LB_NOM"]

    # (cd_ref et cd_nom inchangés)
    zn = load_znieff_info(paths)
    df["ID ZNIEFF"] = df["ID ZNIEFF"].astype(str).str.strip()

    df = df.merge(zn, how="left", on="ID ZNIEFF")

    # Réordonner colonnes (propre)
    df = df[[
        "ID ZNIEFF",
        "Nom ZNIEFF",
        "Type ZNIEFF",
        "Groupe taxonomique",
        "Nom scientifique",
        "CD_REF",
        "CD_NOM",
        "Type espèce",
    ]]

    # Tri lisible
    df = df.sort_values(
        by=["Groupe taxonomique", "Nom scientifique"],
        kind="stable"
    )

    return df


def export_habitats_znieff(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    """Exporte les habitats ZNIEFF filtrés par codes avec groupage et enrichissement."""
    df = filter_parquet(
        parquet_file=paths.znieff_habitats,
        key_col=HABITATS_KEY_COL,
        keep_cols=HABITATS_KEEP_COLS,
        codes=codes,
    )
    # Expect ID_TYPO_INFO to be available in HABITATS_KEEP_COLS
    if df.empty:
        return pd.DataFrame(columns=[
            "ID ZNIEFF", "Nom ZNIEFF", "CD_HAB","Type habitat", "Code typologie", "Libellé typologie",
            "Code EUNIS", "Libellé EUNIS", "Code Corine", "Libellé Corine",
            "Code HIC", "Libellé HIC"
        ])

    # Ensure columns exist and normalize as strings
    for col in ["NM_SFFZN", "CD_TYPO", "LB_TYPO", "CD_HAB", "LB_CODE", "LB_HAB", "ID_TYPO_INFO"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
        else:
            df[col] = ""

    # Load znieff_habitats_info to get FG_TYPO and map its values to labels
    fg_map = {"A": "Autre habitat", "D": "Déterminant", "P": "Périphérique"}
    if hasattr(paths, "znieff_habitats_info") and paths.znieff_habitats_info.exists():
        try:
            typo_info = pd.read_parquet(paths.znieff_habitats_info, columns=["ID_TYPO_INFO", "FG_TYPO"]) 
            typo_info["ID_TYPO_INFO"] = typo_info["ID_TYPO_INFO"].astype(str).str.strip()
            typo_info["FG_TYPO"] = typo_info["FG_TYPO"].astype(str).str.strip()
            fg_lookup = dict(zip(typo_info["ID_TYPO_INFO"], typo_info["FG_TYPO"]))
        except Exception:
            fg_lookup = {}
    else:
        fg_lookup = {}

    out_rows = []
    # Group by ZNIEFF and ID_TYPO_INFO
    grp = df.groupby(["NM_SFFZN", "ID_TYPO_INFO"], sort=False)

    for (zn, id_typo), group in grp:
        # prepare temporary lists to collect codes/libs per typology (avoids repeated string concat)
        enis_codes: list = []
        enis_libs: list = []
        corine_codes: list = []
        corine_libs: list = []
        hic_codes: list = []
        hic_libs: list = []

        for _, r in group.iterrows():
            cd_typo = str(r.get("CD_TYPO", "")).zfill(0) if r.get("CD_TYPO", "") is not None else ""
            cd_typo_norm = cd_typo.lstrip("0")
            code = str(r.get("LB_CODE", "")).strip()
            lib = str(r.get("LB_HAB", "")).strip()

            if cd_typo_norm == "7":
                enis_codes.append(code)
                enis_libs.append(lib)
            elif cd_typo_norm == "22":
                corine_codes.append(code)
                corine_libs.append(lib)
            elif cd_typo_norm == "8":
                hic_codes.append(code)
                hic_libs.append(lib)
            else:
                pass

        row = {
            "ID ZNIEFF": zn,
            "CD_HAB": next((v for v in group["CD_HAB"].tolist() if v and v != "nan"), ""),
            "Code typologie": ";".join(sorted(set(group["CD_TYPO"].tolist()))),
            "Libellé typologie": ";".join(sorted(set(group["LB_TYPO"].tolist()))),
            "Code EUNIS": " | ".join([c for c in enis_codes if c and c != "nan"]),
            "Libellé EUNIS": " | ".join([l for l in enis_libs if l and l != "nan"]),
            "Code Corine": " | ".join([c for c in corine_codes if c and c != "nan"]),
            "Libellé Corine": " | ".join([l for l in corine_libs if l and l != "nan"]),
            "Code HIC": " | ".join([c for c in hic_codes if c and c != "nan"]),
            "Libellé HIC": " | ".join([l for l in hic_libs if l and l != "nan"]),
            "Type habitat": fg_map.get(fg_lookup.get(str(id_typo), ""), ""),
        }

        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)

    # Charger les infos ZNIEFF (nom, type) et faire la jointure
    znieff_info = load_znieff_info(paths)
    znieff_info = znieff_info[["ID ZNIEFF", "Nom ZNIEFF", "Type ZNIEFF"]]  # Garder seulement les colonnes nécessaires
    out_df = out_df.merge(znieff_info, how="left", on="ID ZNIEFF")

    cols = [
        "ID ZNIEFF", "Nom ZNIEFF", "Type ZNIEFF", "Type habitat", "CD_HAB", "Code typologie", "Libellé typologie",
        "Code EUNIS", "Libellé EUNIS", "Code Corine", "Libellé Corine",
        "Code HIC", "Libellé HIC",
    ]

    for c in cols:
        if c not in out_df.columns:
            out_df[c] = ""

    out_df = out_df[cols]

    return out_df
