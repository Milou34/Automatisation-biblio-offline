"""Traitements de filtrage/export ZNIEFF."""

# pylint: disable=duplicate-code

from typing import List, Sequence
import pandas as pd

from src.outils_communs import LocalINPNPaths, ensure_exists, filter_parquet


# Constantes ZNIEFF
ESPECES_KEY_COL = "nm_sffzn"
HABITATS_KEY_COL = "NM_SFFZN"

ESPECES_KEEP_COLS = ["nm_sffzn", "cd_ref", "cd_nom", "fg_esp", "groupe_taxo"]
HABITATS_KEEP_COLS = [
    "NM_SFFZN",
    "CD_TYPO",
    "LB_TYPO",
    "CD_HAB",
    "LB_CODE",
    "LB_HAB",
    "ID_TYPO_INFO",
]


def parse_codes_znieff(raw: str) -> List[str]:
    """
    Analyse et vérification des inputs de codes ZNIEFF.
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


def export_habitats_znieff(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    # pylint: disable=too-many-locals
    """Exporte les habitats ZNIEFF filtrés par codes avec groupage et enrichissement."""
    # Schéma cible de sortie (ordre final des colonnes Excel)
    final_cols = [
        "ID ZNIEFF",
        "Nom ZNIEFF",
        "Type ZNIEFF",
        "Type habitat",
        "CD_HAB",
        "Code typologie",
        "Libellé typologie",
        "Code EUNIS",
        "Libellé EUNIS",
        "Code Corine",
        "Libellé Corine",
        "Code HIC",
        "Libellé HIC",
    ]

    # Sortie rapide si aucun code ZNIEFF valide à traiter
    code_set = {str(c).strip() for c in codes if str(c).strip()}
    if not code_set:
        return pd.DataFrame(columns=final_cols)

    df = filter_parquet(
        parquet_file=paths.znieff_habitats,
        key_col=HABITATS_KEY_COL,
        keep_cols=HABITATS_KEEP_COLS,
        codes=codes,
    )

    if df.empty:
        return pd.DataFrame(columns=final_cols)

    # On garantit la présence des colonnes attendues puis on normalise en texte
    required_cols = [
        "NM_SFFZN",
        "CD_TYPO",
        "LB_TYPO",
        "CD_HAB",
        "LB_CODE",
        "LB_HAB",
        "ID_TYPO_INFO",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()

    fg_map = {"A": "Autre habitat", "D": "Déterminant", "P": "Périphérique"}
    if hasattr(paths, "znieff_habitats_info") and paths.znieff_habitats_info.exists():
        try:
            typo_info = pd.read_parquet(
                paths.znieff_habitats_info,
                columns=["ID_TYPO_INFO", "FG_TYPO"],
            )
            typo_info["ID_TYPO_INFO"] = typo_info["ID_TYPO_INFO"].astype(str).str.strip()
            typo_info["FG_TYPO"] = typo_info["FG_TYPO"].astype(str).str.strip()
            fg_lookup = dict(zip(typo_info["ID_TYPO_INFO"], typo_info["FG_TYPO"]))
        except (OSError, ValueError, KeyError):
            fg_lookup = {}
    else:
        fg_lookup = {}

    group_keys = ["NM_SFFZN", "ID_TYPO_INFO"]

    # Agrégations utilitaires pour construire les champs concaténés
    def join_unique(values: pd.Series) -> str:
        cleaned = sorted({v for v in values if v and v != "nan"})
        return ";".join(cleaned)

    def join_pipe(values: pd.Series) -> str:
        cleaned = [v for v in values if v and v != "nan"]
        return " | ".join(cleaned)

    def first_valid(values: pd.Series) -> str:
        for value in values:
            if value and value != "nan":
                return value
        return ""

    df["CD_TYPO_NORM"] = df["CD_TYPO"].str.lstrip("0")

    # Agrégation principale par ZNIEFF + typologie
    out_df = (
        df.groupby(group_keys, sort=False)
        .agg(
            CD_HAB=("CD_HAB", first_valid),
            **{
                "Code typologie": ("CD_TYPO", join_unique),
                "Libellé typologie": ("LB_TYPO", join_unique),
            },
        )
        .reset_index()
    )

    # Agrégations spécialisées par famille de typologie
    eunis = df[df["CD_TYPO_NORM"] == "7"].groupby(group_keys, sort=False).agg(
        **{
            "Code EUNIS": ("LB_CODE", join_pipe),
            "Libellé EUNIS": ("LB_HAB", join_pipe),
        }
    )
    corine = df[df["CD_TYPO_NORM"] == "22"].groupby(group_keys, sort=False).agg(
        **{
            "Code Corine": ("LB_CODE", join_pipe),
            "Libellé Corine": ("LB_HAB", join_pipe),
        }
    )
    hic = df[df["CD_TYPO_NORM"] == "8"].groupby(group_keys, sort=False).agg(
        **{
            "Code HIC": ("LB_CODE", join_pipe),
            "Libellé HIC": ("LB_HAB", join_pipe),
        }
    )

    out_df = out_df.merge(eunis, how="left", left_on=group_keys, right_index=True)
    out_df = out_df.merge(corine, how="left", left_on=group_keys, right_index=True)
    out_df = out_df.merge(hic, how="left", left_on=group_keys, right_index=True)

    # Mapping du type habitat à partir de l'identifiant de typologie
    out_df = out_df.rename(columns={"NM_SFFZN": "ID ZNIEFF"})
    out_df["Type habitat"] = out_df["ID_TYPO_INFO"].map(
        lambda value: fg_map.get(fg_lookup.get(str(value), ""), "")
    )

    # Charger les infos ZNIEFF (nom, type) et faire la jointure
    znieff_info = load_znieff_info(paths)
    znieff_info = znieff_info[["ID ZNIEFF", "Nom ZNIEFF", "Type ZNIEFF"]]
    out_df = out_df.merge(znieff_info, how="left", on="ID ZNIEFF")

    for c in final_cols:
        if c not in out_df.columns:
            out_df[c] = ""

    # Sécurisation finale des valeurs nulles et de l'ordre des colonnes
    out_df = out_df.fillna("")
    out_df = out_df[final_cols]

    return out_df


def export_especes_znieff(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    """Exporte les espèces ZNIEFF filtrées par codes."""
    # Schéma cible de sortie (ordre final des colonnes Excel)
    final_cols = [
        "ID ZNIEFF",
        "Nom ZNIEFF",
        "Type ZNIEFF",
        "Groupe taxonomique",
        "Nom scientifique",
        "CD_REF",
        "CD_NOM",
        "Type espèce",
    ]

    # Sortie rapide si aucun code ZNIEFF valide à traiter
    code_set = {str(c).strip() for c in codes if str(c).strip()}
    if not code_set:
        return pd.DataFrame(columns=final_cols)

    df = filter_parquet(
        parquet_file=paths.znieff_espece,
        key_col=ESPECES_KEY_COL,
        keep_cols=ESPECES_KEEP_COLS,
        codes=codes,
    )

    # Si aucun résultat après filtrage, on renvoie un tableau vide mais structuré
    if df.empty:
        return pd.DataFrame(columns=final_cols)

    # TAXREF: jointure sur cd_nom -> CD_NOM pour récupérer LB_NOM
    ensure_exists(paths.taxref)
    tax = pd.read_parquet(paths.taxref, columns=["CD_NOM", "LB_NOM"])

    df["cd_nom"] = df["cd_nom"].astype(str).str.strip()
    df["cd_ref"] = df["cd_ref"].astype(str).str.strip()
    df["nm_sffzn"] = df["nm_sffzn"].astype(str).str.strip()
    tax["CD_NOM"] = tax["CD_NOM"].astype(str).str.strip()

    df = df.merge(tax, how="left", left_on="cd_nom", right_on="CD_NOM").drop(columns=["CD_NOM"])

    # Mapping fg_esp -> libellé
    fg_map = {
        "A": "Autre espèce",
        "E": "Autre espèce à enjeux",
        "D": "Déterminante",
        "C": "Confidentielle",
    }
    df["fg_esp"] = df["fg_esp"].astype(str).str.strip().map(fg_map).fillna(df["fg_esp"])

    # Renommage final des colonnes métier
    df = df.rename(columns={
        "nm_sffzn": "ID ZNIEFF",
        "groupe_taxo": "Groupe taxonomique",
        "LB_NOM": "Nom scientifique",
        "cd_ref": "CD_REF",
        "cd_nom": "CD_NOM",
        "fg_esp": "Type espèce",
    })

    zn = load_znieff_info(paths)
    df = df.merge(zn, how="left", on="ID ZNIEFF")

    # Sélection stricte de la structure de sortie
    df = df[final_cols]

    # Tri lisible
    df = df.sort_values(
        by=["Groupe taxonomique", "Nom scientifique"],
        kind="stable"
    )

    return df


