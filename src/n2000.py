"""Traitements de filtrage/export Natura 2000."""

# pylint: disable=duplicate-code

from typing import List, Sequence
import pandas as pd

from src.outils_communs import LocalINPNPaths, ensure_exists


def parse_codes_n2000(raw: str) -> List[str]:
    """
    Analyse et vérification des inputs de codes N2000.
    Accepte des codes Natura 2000 séparés par ; , retours ligne, tabulations.
    Format attendu : FR + 7 chiffres (ex: FR1234567).
    Dé-doublonne en conservant l'ordre.
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

    # Validation: chaque code N2000 doit être composé de "FR" + 7 chiffres
    for c in out:
        if not (c.startswith("FR") and len(c) == 9 and c[2:].isdigit()):
            raise ValueError(
                "Code N2000 invalide: "
                f"'{c}'. Un code N2000 doit être composé de 'FR' suivi de 7 chiffres."
            )

    return out


def load_n2000_info(paths: LocalINPNPaths) -> pd.DataFrame:
    """Charge les informations générales N2000 et normalise les colonnes utiles."""
    ensure_exists(paths.n2000_infos_generales)

    infos = pd.read_parquet(
        paths.n2000_infos_generales,
        columns=["sitecode", "site_name", "type"]
    )

    infos["sitecode"] = infos["sitecode"].astype(str).str.strip().str.upper()

    # Mapper type : A -> ZPS, B -> pSIC/SIC/ZSC
    type_map = {"A": "ZPS", "B": "pSIC/SIC/ZSC"}
    infos["type"] = (
        infos["type"]
        .astype(str)
        .str.strip()
        .map(type_map)
        .fillna(infos["type"])
    )

    return infos


def export_habitats_n2000(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    """
    Exporte les habitats Natura 2000 en filtrant sur sitecode.
    Récupère sitecode, cd_ue, cd_hab depuis N2000_Habitats.
    Croise avec HABREF_70 sur cd_hab pour ajouter LB_HAB_FR.
    Croise avec N2000_Infos_generales pour ajouter site_name et type.
    """
    final_cols = [
        "ID N2000",
        "Nom site",
        "Type de zone",
        "Code HIC",
        "Libellé HIC",
        "Forme prioritaire",
        "CD_HAB",
    ]

    # Sortie rapide si aucun code N2000 à traiter
    code_set = {str(c).strip().upper() for c in codes if str(c).strip()}
    if not code_set:
        return pd.DataFrame(columns=final_cols)

    ensure_exists(paths.n2000_habitats)
    df = pd.read_parquet(
        paths.n2000_habitats,
        columns=["sitecode", "cd_ue", "cd_hab", "pf"]
    )

    # Normaliser et filtrer sur les codes demandés
    df["sitecode"] = df["sitecode"].astype(str).str.strip().str.upper()
    df = df[df["sitecode"].isin(code_set)]

    if df.empty:
        return pd.DataFrame(columns=final_cols)

    ensure_exists(paths.habref_70)
    habref = pd.read_parquet(
        paths.habref_70,
        columns=["CD_HAB", "LB_HAB_FR"]
    )

    # Normaliser les clés de jointure
    df["cd_hab"] = df["cd_hab"].astype(str).str.strip()
    habref["CD_HAB"] = habref["CD_HAB"].astype(str).str.strip()

    # Jointures d'enrichissement (HABREF + infos N2000)
    df = df.merge(
        habref,
        how="left",
        left_on="cd_hab",
        right_on="CD_HAB",
    ).drop(columns=["CD_HAB"])
    df = df.merge(load_n2000_info(paths), how="left", on="sitecode")

    # Renommage métier des colonnes
    df = df.rename(columns={
        "sitecode": "ID N2000",
        "cd_ue": "Code HIC",
        "cd_hab": "CD_HAB",
        "LB_HAB_FR": "Libellé HIC",
        "pf": "Forme prioritaire",
        "site_name": "Nom site",
        "type": "Type de zone",
    })

    # Mapping Forme prioritaire : true/false -> Oui/Non
    pf_map = {"true": "Oui", "false": "Non"}
    df["Forme prioritaire"] = (
        df["Forme prioritaire"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map(pf_map)
        .fillna(df["Forme prioritaire"])
    )

    return df[final_cols]


def export_especes_n2000(paths: LocalINPNPaths, codes: Sequence[str]) -> pd.DataFrame:
    """
    Exporte les espèces Natura 2000 (inscrites et autres).
    Filtre sur sitecode, concatène les deux tables, ajoute un "Type espèce",
    jointure avec TAXREF pour récupérer LB_NOM,
    jointure avec N2000_Infos_generales pour ajouter site_name et type.
    """
    final_cols = [
        "ID N2000",
        "Nom site",
        "Type de zone",
        "Groupe taxonomique",
        "Nom scientifique",
        "CD_NOM",
        "CD_REF",
        "Type espèce",
    ]

    # Sortie rapide si aucun code N2000 à traiter
    code_set = {str(c).strip().upper() for c in codes if str(c).strip()}
    if not code_set:
        return pd.DataFrame(columns=final_cols)

    ensure_exists(paths.n2000_especes_inscrites)
    ensure_exists(paths.n2000_especes_autres)

    # Mapping taxgroup
    taxgroup_map = {
        "A": "Amphibiens",
        "B": "Oiseaux",
        "F": "Poissons",
        "I": "Invertébrés",
        "M": "Mammifères",
        "P": "Plantes",
        "R": "Reptiles",
    }

    # Lire les deux tables d'espèces
    df_inscrites = pd.read_parquet(
        paths.n2000_especes_inscrites,
        columns=["sitecode", "cd_nom", "cd_ref", "taxgroup"]
    )
    df_autres = pd.read_parquet(
        paths.n2000_especes_autres,
        columns=["sitecode", "cd_nom", "cd_ref", "taxgroup"]
    )

    # Ajouter colonne "Type espèce"
    df_inscrites["Type espèce"] = "Espèce inscrite"
    df_autres["Type espèce"] = "Espèce autre"

    # Concaténer les deux tables
    df = pd.concat([df_inscrites, df_autres], ignore_index=True)

    # Normaliser et filtrer sur sitecode
    df["sitecode"] = df["sitecode"].astype(str).str.strip().str.upper()
    df = df[df["sitecode"].isin(code_set)]

    if df.empty:
        return pd.DataFrame(columns=final_cols)

    # Normaliser les colonnes pour les jointures
    df["cd_nom"] = df["cd_nom"].astype(str).str.strip()
    df["cd_ref"] = df["cd_ref"].astype(str).str.strip()
    df["taxgroup"] = df["taxgroup"].astype(str).str.strip()

    # Jointure avec TAXREF sur cd_nom -> CD_NOM
    ensure_exists(paths.taxref)
    tax = pd.read_parquet(paths.taxref, columns=["CD_NOM", "LB_NOM"])
    tax["CD_NOM"] = tax["CD_NOM"].astype(str).str.strip()

    df = df.merge(tax, how="left", left_on="cd_nom", right_on="CD_NOM").drop(columns=["CD_NOM"])

    # Mapper taxgroup aux libellés
    df["taxgroup"] = df["taxgroup"].map(taxgroup_map).fillna(df["taxgroup"])

    # Jointure avec les infos N2000 normalisées
    df = df.merge(load_n2000_info(paths), how="left", on="sitecode")

    # Renommage métier des colonnes
    df = df.rename(columns={
        "sitecode": "ID N2000",
        "taxgroup": "Groupe taxonomique",
        "cd_nom": "CD_NOM",
        "cd_ref": "CD_REF",
        "site_name": "Nom site",
        "type": "Type de zone",
        "LB_NOM": "Nom scientifique",
    })

    return df[final_cols]

