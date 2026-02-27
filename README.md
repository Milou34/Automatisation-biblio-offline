# Automatisation bibliographie offline

<a href="https://github.com/Milou34/Automatisation-biblio-offline/actions/workflows/build-executable-create-release.yml" target="_blank">![Build Status](https://github.com/Milou34/Automatisation-biblio-offline/actions/workflows/build-executable-create-release.yml/badge.svg)</a>
<a href="https://github.com/Milou34/Automatisation-biblio-offline/actions/workflows/pylint.yml" target="_blank">![Code Quality](https://github.com/Milou34/Automatisation-biblio-offline/actions/workflows/pylint.yml/badge.svg)</a>
<a href="https://github.com/Milou34/Automatisation-biblio/blob/main/LICENSE.txt" target="_blank">![Licence](https://img.shields.io/badge/Licence-Apache_2.0-blue.svg)</a>

<a href="https://www.python.org/doc" target="_blank">![Python](https://img.shields.io/badge/Python-3.12-ffd343?logo=python)</a>
<a href="https://pypi.org/project/openpyxl" target="_blank">![Openpyxl](https://img.shields.io/badge/Openpyxl-3.1.5-ffd343?logo=pypi)</a>
<a href="https://pypi.org/project/pandas/" target="_blank">![Pandas](https://img.shields.io/badge/Pandas-3.0.0-ffd343?logo=pypi)</a>
<a href="https://pypi.org/project/fastparquet/" target="_blank">![Fastparquet](https://img.shields.io/badge/Fastparquet-2025.12.0-ffd343?logo=pypi)</a>

## Bibliographie automatisée offline pour des études environnementales

Ce projet Python a pour objectif d’automatiser l’export des données espèces et habitats ZNIEFF et Natura 2000 issues de la bibliographie de l’INPN, dans le cadre d’études environnementales. Il permet de centraliser la recherche, le formatage et l’organisation des sources, tout en facilitant l’intégration des références dans des feuilles de calcul.

## Fonctionnalités

- **Formatage des données** : Pipeline de traitement et d'extraction des données issues de fichiers locaux, reposant notamment sur le filtrage, la sélection et la normalisation des colonnes utiles.
- **Création de plusieurs feuilles de calcul et intégration des données** : Création et insertion des habitats et espèces ZNIEFF et habitats et espèces Natura 2000 dans des onglets distincts. Ces feuilles sont intégrées dans un excel lisible et renommé.
- **Renommage du fichier de sortie** : Génération automatique du nom du fichier à partir du nom saisi par l’utilisateur et de la date du jour.
- **Création du fichier Excel** : Enregistrement du fichier Excel dans le chemin indiqué par l’utilisateur.
- **Fonctionnement hors ligne** : Ce programme est prévu pour fonctionner en local sans besoin de ressources distantes. Les bases de données utilisées sont directement intégrées dans l'exécutable.
- ***Sources des données*** : Les données sont issues de la page temporaire de téléchargement de Patrinat (dossiers ZNIEFF, Natura 2000, TAXREF et HABREF) : https://www.patrinat.fr/fr/page-temporaire-de-telechargement-des-referentiels-de-donnees-lies-linpn-7353

## Utilisation

Avant de commencer, assurez-vous de suivre les étapes suivantes :


1. Télécharger <a href="https://github.com/Milou34/Automatisation-biblio-offline/releases/latest" target="_blank">l'executable</a> (cliquer sur le .exe)
2. Préalablement au lancement du programme, pour le ou les projets dont vous souhaitez créer la bibliographie, assurez vous d'avoir bien créé la couche `zonages_aires_detude` à l'aide du modèle Zonages sur QGIS. Dans la table attributaire de cette couche, vous pourrez retrouver les `codes ZNIEFF` et `Natura 2000` demandés par le programme.
3. Lancez l'exécutable `v-2-0-...-bibliographie-zonage`.
4. A la première exécution du programme, si une fenêtre d'alerte Windows apparaît, cliquez sur `Informations complémentaires`, puis sur `Exécuter quand même`. La fenêtre de terminal s'ouvre.

⚠️ **Attention** : Un temps de latence est présent au lancement du programme, attendre quelques secondes avant l'apparition du premier input.

   Suivez les instructions qui s'affichent dans la console : 

5. Entrer les codes des ZNIEFFs de types 1 et 2 présentes dans l'AER, qui sont notés dans la colonne `ID_MNHN` de la couche `zonages_aires_detude` dans QGIS.\
Puis appuyer sur `Entrer`.

    S'il n'y a pas de code ZNIEFF à renseigner, appuyer seulement sur `Entrer` et poursuivez le programme.

    S'il y a une erreur sur l'un des codes (s'ils n'ont pas exactement 9 chiffres), les codes sont redemandés.\
    **Attention à bien renseigner uniquement des codes ZNIEFF.**

6. Entrer les codes des zones Natura 2000 (les SIC et ZPS) présentes dans l'AEE, qui sont notés dans la colonne `SITE_CODE` de la couche `zonages_aires_detude` dans QGIS.\
Puis appuyer sur `Entrer`.

    S'il n'y a pas de code Natura 2000 à renseigner, appuyer seulement sur `Entrer` et poursuivez le programme.

    S'il y a une erreur sur l'un des codes (s'ils ne commencent pas par FR suivi de 7 chiffres exactement), les codes sont redemandés.\
    **Attention à bien renseigner uniquement des codes Natura 2000.**
   
Il est necessaire de remplir au moins un code pour poursuivre le programme. En l'absence d'au moins un code, les input ZNIEFF et Natura 2000 seront redemandés.

8. Entrer le nom du projet, cela permettra de renommer automatiquement l'excel final en : `Bibliographie + Nom du projet + Date`.\
Puis appuyer sur `Entrer`.

9. Renseignez le chemin du dossier de destination où sera créé le Excel final.

Copiez/collez le chemin du dossier dans la console (clic droit sur le chemin en haut de la fenêtre, `copier l'adresse`) et écrire à la suite le nom du dossier à créer si besoin.\
    Exemple : `C:\Users\PrénomNOM\Documents\ProjetX-Bibliographie`\
    Puis appuyer sur `Entrer`.

   ⚠️ **ATTENTION! Parfois OneDrive peut provoquer une erreur dû aux droits d'accès.** ⚠️ \
    **Tips si ça vous arrive** : Ouvrez l'explorateur de fichiers puis allez dans :
    Ce PC > Windows (C:) > Utilisateurs > PrénomNOM > 
    Puis créez un dossier `Documents` à cet endroit (en local hors OneDrive). Travaillez alors depuis ce dossier pour créer vos Excels de bibliographie.

11. Le programme génère l'excel final dans le dossier demandé.

12. Si vous souhaitez poursuivre avec la bibliographie d'un autre projet, appuyez sur `O`. Sinon, appuyez sur `N` pour quitter le programme.

## Source et structure des données

Le programme utilise les données mises à disposition sur la page temporaire de téléchargement de [PatriNat](https://www.patrinat.fr/fr/page-temporaire-de-telechargement-des-referentiels-de-donnees-lies-linpn-7353). Ces données sont fournies sous forme de fichiers `.csv` contenus dans les archives `.zip` des référentiels **ZNIEFF BDD.zip**, **NATURA 2000.zip**, **TAXREF v18 2025.zip** et **HABREF.zip**.

Les référentiels **ZNIEFF** et **Natura 2000** contiennent les données bibliographiques associées aux sites, mais uniquement sous forme d’identifiants. Les référentiels **TAXREF** et **HABREF** sont utilisés en complément afin de restituer les noms scientifiques des espèces et les libellés des habitats, par le biais de jointures réalisées respectivement sur les codes espèces (`CD_NOM`) et les codes habitats (`CD_HAB`).

Avant d’être utilisées par le programme, les données issues des fichiers `.csv` sont converties en fichiers `.parquet`. Compte tenu du volume des tableaux de données et des traitements automatisés réalisés, le traitement direct des fichiers `.csv` n’était techniquement pas envisageable : leurs temps de lecture, de filtrage et de jointure étaient incompatibles avec le fonctionnement du programme.

Le format `.parquet`, grâce à sa structure colonnaire et compressée, permet des temps de calcul nettement plus rapides, une meilleure gestion des types de données et une réduction significative de la taille des fichiers, rendant ainsi les traitements fiables et reproductibles.

Si, ultérieurement, de nouveaux `.csv` doivent être intégrés à cause d’une mise à jour des sources, il faudra réaliser à nouveau la conversion des fichiers nécessaires en `.parquet` et les remplacer dans le dossier `data`. Lors de cette opération, veillez à conserver **les mêmes noms de fichiers** (ou à adapter les noms dans le code du programme si nécessaire).

### Nommage des fichiers de données

Pour chaque fichier, le nom **avant la flèche** correspond au nom du fichier tel que présent dans l’archive, et le nom **après la flèche** correspond au nom attendu dans le dossier `data` **après conversion au format `.parquet`**.

#### Référentiel ZNIEFF
- `REF_ESPECE.csv` → `ZNIEFF_Especes.parquet`
- `REF_TYPO.csv` → `ZNIEFF_Habitats.parquet`
- `REF_TYPO_INFO.csv` → `ZNIEFF_Habitats_Infos.parquet`
- `REF_ZNIEFF.csv` → `ZNIEFF_Infos_generales.parquet`

#### Référentiel Natura 2000
- `species.csv` → `N2000_Especes_inscrites.parquet`
- `species_other.csv` → `N2000_Especes_autres.parquet`
- `habit1.csv` → `N2000_Habitats.parquet`
- `biotop.csv` → `N2000_Infos_generales.parquet`

#### Référentiel HABREF
- `HABREF_70.csv` → `HABREF_70.parquet`

#### Référentiel TAXREF (cas particulier)

Le référentiel TAXREF est fourni sous la forme d’un fichier texte (`.txt`). Une étape intermédiaire de conversion en `.csv` est nécessaire avant la conversion finale en `.parquet`. Cette conversion doit être réalisée en veillant à utiliser un encodage `UTF-8` et un séparateur `,`, afin de garantir une structure homogène avec les autres fichiers `.csv` et d’éviter toute erreur de lecture ou de traitement.

- `TAXREFv18.txt` → `TAXREFv18.parquet`

## Structure du Projet

```
.
├── README.md               # Documentation du projet
├── requirements.txt        # Dépendances du projet
├── favicon.ico             # icon de l'executable
├── LICENCE.txt             # Condition d'utilisation
├── main.py                 # Script principal
├── .gitignore              # Ignorer les fichiers compilés
├── .github/                # Dossier de configuration du repository
└── src/                    # Dossier du code source
```

## Authors

- <a href="https://www.linkedin.com/in/marylou-bertin" target="_blank">**Marylou Bertin**</a> : Cheffe de Projet & Developpeuse
- <a href="https://www.linkedin.com/in/valentin-guyon" target="_blank">**Valentin Guyon**</a> : Lead Developper & DevOps

## Licence
Ce projet est sous licence Apache 2.0 Voir le fichier <a href="./LICENSE.txt" target="_blank">`LICENSE.txt`</a> pour plus de détails.
