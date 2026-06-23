# 🛒 Pipeline ETL E-commerce — Online Retail Dataset UCI

> Pipeline industrialisé de traitement des ventes e-commerce basé sur le **vrai dataset UCI Online Retail** (541 909 transactions réelles), orchestré par Apache Airflow, automatisé par Jenkins CI/CD et stocké dans MongoDB.

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.0-green)
![Jenkins](https://img.shields.io/badge/Jenkins-LTS-red)
![MongoDB](https://img.shields.io/badge/MongoDB-7-brightgreen)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Dataset](https://img.shields.io/badge/Dataset-UCI%20Online%20Retail-orange)

---

## 📊 Dataset — UCI Online Retail

| Propriété | Valeur |
|-----------|--------|
| Source | [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail) |
| Volume | 541 909 transactions |
| Taille | 46 Mo |
| Période | 01/12/2010 — 09/12/2011 |
| Pays couverts | 37 pays |
| Colonnes | InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country |

---

## 🏗️ Architecture

```
Développeur
     │
     └── git push
              │
         ┌───▼───┐
         │Jenkins│  ← Tests + Validation + Déploiement DAG
         └───┬───┘
              │
         ┌───▼──────┐
         │  Airflow  │  ← Orchestration ETL (46 tâches)
         └───┬──────┘
              │
    ┌─────────▼──────────────────────────────┐
    │  wait_for_file  (FileSensor)            │
    │  validate_file  (541 909 lignes)        │
    │  quality_check  (règles métier)         │
    │  check_data     (BranchPythonOperator)  │
    │  load_data      (KPI globaux)           │
    │  analyse_france / analyse_germany /...  │  ← 37 tâches dynamiques
    │  generate_report (TriggerRule.ALL_DONE) │
    │  store_mongodb  (métriques complètes)   │
    └─────────┬──────────────────────────────┘
              │
         ┌───▼──────┐
         │ MongoDB  │  ← ecommerce_analytics.sales_metrics
         └──────────┘
```

---

## 🚀 Stack Technique

| Composant | Version | Rôle |
|-----------|---------|------|
| Apache Airflow | 2.9.0 | Orchestration pipeline ETL |
| Jenkins | LTS | CI/CD — tests, validation, déploiement |
| MongoDB | 7 | Stockage des métriques et KPI |
| PostgreSQL | 15 | Metadata DB Airflow |
| Docker Compose | - | Conteneurisation |
| Python | 3.12 | Logique métier et tests unitaires |
| pytest | 8.4.1 | 10 tests unitaires (10/10 PASSED) |

---

## 📁 Structure du projet

```
airflow-ecommerce-uci/
├── dags/
│   └── ecommerce_sales_pipeline.py    # DAG principal — 46 tâches
├── tests/
│   └── test_pipeline.py               # 10 tests unitaires pytest
├── data/
│   └── dataset.csv                    # UCI Online Retail (541 909 lignes)
├── scripts/
│   └── check_mongodb.py               # Vérification MongoDB
├── Jenkinsfile                        # Pipeline CI/CD — 6 stages
├── requirements.txt                   # Dépendances Python
├── docker-compose.yml                 # Stack Docker complète
└── README.md
```

---

## ⚡ Démarrage rapide

### Prérequis

- Docker Desktop
- Git
- Python 3.12+
- Dataset UCI : [Télécharger ici](https://archive.ics.uci.edu/dataset/352/online+retail)

### Installation

```bash
# Cloner le dépôt
git clone https://github.com/bipanda93/airflow-ecommerce-uci.git
cd airflow-ecommerce-uci

# Créer les dossiers
mkdir -p dags logs data

# Convertir le dataset Excel en CSV
python3 -c "
import pandas as pd
df = pd.read_excel('Online Retail.xlsx')
df.to_csv('data/dataset.csv', index=False)
print(f'{len(df)} lignes converties !')
"

# Démarrer le stack
docker-compose up -d

# Copier le dataset dans le container
docker cp data/dataset.csv ecommerce_airflow:/opt/airflow/data/dataset.csv
```

### Accès

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8183 | admin / admin |
| Jenkins UI | http://localhost:8080 | - |
| MongoDB | localhost:27018 | - |
| PostgreSQL | localhost:5442 | airflow / airflow |

### Configuration Airflow

```
Admin → Connections → +
Connection Id : fs_default
Connection Type : File (path)
Path : /
```

---

## 🧪 Tests unitaires

```bash
python3 -m pytest tests/test_pipeline.py -v
```

**10/10 PASSED en 0.03s**

| Test | Description |
|------|-------------|
| test_file_exists | Présence du fichier dataset.csv |
| test_file_not_empty | Fichier non vide |
| test_file_has_required_columns | 6 colonnes obligatoires validées |
| test_quality_check_detects_invalid_rows | Détection quantités négatives |
| test_quality_check_valid_rows | Identification lignes valides |
| test_chiffre_affaires_calcul | Calcul CA correct |
| test_panier_moyen_calcul | CA / nb_commandes = résultat attendu |
| test_montant_negatif_rejete | Rejet montants négatifs |
| test_regions_extraction | Extraction régions sans doublons |
| test_task_id_format | Format task_id dynamique valide |

---

## 🔧 Pipeline Jenkins — 6 stages (~5 secondes)

| Stage | Durée | Résultat |
|-------|-------|---------|
| Checkout | ~490ms | DAG trouvé |
| Install dependencies | ~416ms | OK |
| Run tests | ~557ms | 10/10 PASSED |
| Validate DAG | ~565ms | 46 tâches validées |
| Deploy DAG | ~401ms | Volume partagé confirmé |
| Verify MongoDB | ~382ms | Container accessible |

---

## 📊 Concepts Airflow implémentés

| Concept | Implémentation |
|---------|----------------|
| `FileSensor` | Détection CSV avec `mode='reschedule'` — libère le worker entre chaque check |
| `BranchPythonOperator` | Routing conditionnel selon qualité des données |
| `XComs` | Transmission KPI entre `quality_check`, `load_data`, `store_mongodb` |
| `Dynamic Tasks` | **37 tâches** créées automatiquement — une par pays détecté dans le CSV |
| `TriggerRule.ALL_DONE` | `generate_report` s'exécute même en cas d'erreur partielle |
| `default_args retries=2` | 2 tentatives avec 30s d'attente sur toutes les tâches |

---

## ⚙️ Optimisation mémoire — Itérateur vs list()

Avec 541 909 lignes, charger tout en mémoire avec `list(reader)` provoquerait un **OutOfMemoryError**.

```python
# ❌ Problématique avec 541 909 lignes
rows = list(reader)  # ~500 Mo en RAM d'un coup

# ✅ Optimisé — traitement ligne par ligne
for row in reader:   # ~quelques Ko en RAM
    chiffre_affaires += float(row['Quantity']) * float(row['UnitPrice'])
```

L'itérateur est suffisant ici car les opérations sont des **accumulations simples** (somme, set, dictionnaire). Les chunks pandas seraient nécessaires pour des opérations de **groupby complexe** sur plusieurs lignes simultanées.

---

## 📈 Résultats — Dataset UCI complet

```json
{
  "nb_commandes": 18536,
  "nb_clients": 4339,
  "chiffre_affaires": 8911407.90,
  "panier_moyen": 480.76,
  "lignes_valides": 397924,
  "lignes_invalides": 143985
}
```

### Top 10 produits

| Produit | CA (£) |
|---------|--------|
| PAPER CRAFT, LITTLE BIRDIE | 168 469.60 |
| REGENCY CAKESTAND 3 TIER | 142 592.95 |
| WHITE HANGING HEART T-LIGHT HOLDER | 100 448.15 |
| JUMBO BAG RED RETROSPOT | 85 220.78 |
| MEDIUM CERAMIC TOP STORAGE JAR | 81 416.73 |
| POSTAGE | 77 803.96 |
| PARTY BUNTING | 68 844.33 |
| ASSORTED COLOUR BIRD ORNAMENT | 56 580.34 |
| Manual | 53 779.93 |
| RABBIT NIGHT LIGHT | 51 346.20 |

### Top régions

| Région | CA (£) |
|--------|--------|
| United Kingdom | 7 308 391.55 |
| Netherlands | 285 446.34 |
| EIRE | 265 545.90 |
| Germany | 228 867.14 |
| France | 209 024.05 |
| Australia | 138 521.31 |

---

## 🗄️ Document MongoDB

```json
{
  "execution_date": "2026-06-23T11:19:48",
  "dag_id": "ecommerce_sales_pipeline",
  "dataset": "online_retail_uci",
  "status": "success",
  "global_metrics": {
    "nb_commandes": 18536,
    "nb_clients": 4339,
    "chiffre_affaires": 8911407.90,
    "panier_moyen": 480.76
  },
  "top_products": [...],
  "region_metrics": [...],
  "quality": {
    "valid_rows": 397924,
    "invalid_rows": 143985
  }
}
```

---

## ⚠️ Limites et perspectives

| Limite actuelle | Solution production |
|----------------|---------------------|
| LocalExecutor (1 machine) | CeleryExecutor / KubernetesExecutor |
| Itérateur CSV simple | Apache Spark pour transformations complexes |
| Fichiers locaux | MinIO / S3 (Data Lake Bronze/Silver/Gold) |
| Pas de monitoring | Prometheus + Grafana |
| Qualité données basique | Great Expectations |

---

## 🧰 Compétences démontrées

- **Langages** — Python | SQL
- **DevOps & Infra** — Docker | Docker Compose | Git | Jenkins | CI/CD
- **Data Engineering** — Airflow | ETL/ELT | Pipeline CI/CD | Optimisation mémoire
- **Bases de données** — PostgreSQL | MongoDB
- **Méthodes** — Tests unitaires pytest | DataOps | GitOps

---

## 👤 Auteur

**Franck Ulrich BIPANDA**
Mastère Data Engineering — Digital School de Paris (Bac+5, RNCP Niveau 7)

[![GitHub](https://img.shields.io/badge/GitHub-bipanda93-black)](https://github.com/bipanda93)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-franck--bipanda-blue)](https://www.linkedin.com/in/franck-bipanda-13392372)
