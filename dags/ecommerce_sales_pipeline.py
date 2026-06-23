from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import csv
import os

# ============================================================
# CONFIGURATION DU DAG
# ============================================================
default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': lambda context: print(
        f"ALERTE : tâche {context['task_instance'].task_id} a échoué !"
    )
}

dag = DAG(
    dag_id='ecommerce_sales_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Pipeline ETL e-commerce avec Jenkins, Airflow et MongoDB — Online Retail Dataset UCI'
)

# ============================================================
# PARTIE 1 : FILESENSOR
# ============================================================
wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='/opt/airflow/data/dataset.csv',
    poke_interval=15,
    timeout=300,
    mode='reschedule',
    dag=dag
)

# ============================================================
# PARTIE 2 : VALIDATION DU FICHIER
# ============================================================
def validate_file(**context):
    filepath = '/opt/airflow/data/dataset.csv'
    if not os.path.exists(filepath):
        raise Exception(f"Fichier introuvable : {filepath}")
    if os.path.getsize(filepath) == 0:
        raise Exception("Fichier vide !")
    with open(filepath) as f:
        reader = csv.reader(f)
        rows = list(reader)
        nb_lignes = len(rows) - 1
    print(f"Fichier valide : {nb_lignes} enregistrements trouvés")
    return nb_lignes

task_validate = PythonOperator(
    task_id='validate_file',
    python_callable=validate_file,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 3 : CONTRÔLE QUALITÉ
# ============================================================
def quality_check(**context):
    filepath = '/opt/airflow/data/dataset.csv'
    valid_rows = 0
    invalid_rows = 0
    error_records = []

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            errors = []
            try:
                quantity = float(row['Quantity'])
                unit_price = float(row['UnitPrice'])
                montant = quantity * unit_price
                if quantity <= 0:
                    errors.append("Quantité nulle ou négative")
                if montant < 0:
                    errors.append("Montant négatif")
                if not row['InvoiceNo']:
                    errors.append("InvoiceNo manquant")
                if not row['CustomerID'] or row['CustomerID'] == 'nan':
                    errors.append("CustomerID manquant")
            except ValueError:
                errors.append("Valeur non numérique")

            if errors:
                invalid_rows += 1
                if len(error_records) < 1000:
                    row['errors'] = ' | '.join(errors)
                    error_records.append(row)
            else:
                valid_rows += 1

    error_file = '/opt/airflow/data/errors.csv'
    if error_records:
        with open(error_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=error_records[0].keys())
            writer.writeheader()
            writer.writerows(error_records)

    print(f"Lignes valides   : {valid_rows}")
    print(f"Lignes invalides : {invalid_rows}")

    return {
        'valid_rows': valid_rows,
        'invalid_rows': invalid_rows,
        'error_file': error_file if error_records else None
    }

task_quality = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 4 : BRANCHPYTHONOPERATOR
# ============================================================
def check_data(**context):
    quality = context['ti'].xcom_pull(task_ids='quality_check')
    if quality['valid_rows'] > 0:
        return 'load_data'
    else:
        return 'stop_pipeline'

task_branch = BranchPythonOperator(
    task_id='check_data',
    python_callable=check_data,
    provide_context=True,
    dag=dag
)

task_stop = DummyOperator(
    task_id='stop_pipeline',
    dag=dag
)

# ============================================================
# PARTIE 5 : CALCUL DES KPI — traitement ligne par ligne
# ============================================================
def load_data(**context):
    filepath = '/opt/airflow/data/dataset.csv'

    nb_commandes_set = set()
    nb_clients_set = set()
    chiffre_affaires = 0.0
    produits = {}
    regions = {}
    nb_valides = 0

    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                quantity = float(row['Quantity'])
                unit_price = float(row['UnitPrice'])
                if quantity <= 0 or unit_price < 0:
                    continue
                if not row['CustomerID'] or row['CustomerID'] == 'nan':
                    continue
                montant = quantity * unit_price
                nb_commandes_set.add(row['InvoiceNo'])
                nb_clients_set.add(row['CustomerID'])
                chiffre_affaires += montant
                produits[row['Description']] = produits.get(row['Description'], 0) + montant
                regions[row['Country']] = regions.get(row['Country'], 0) + montant
                nb_valides += 1
            except (ValueError, KeyError):
                continue

    nb_commandes = len(nb_commandes_set)
    nb_clients = len(nb_clients_set)
    chiffre_affaires = round(chiffre_affaires, 2)
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes > 0 else 0
    top_products = sorted(produits.items(), key=lambda x: x[1], reverse=True)[:10]

    print(f"Nb commandes       : {nb_commandes}")
    print(f"Nb clients         : {nb_clients}")
    print(f"Chiffre d'affaires : {chiffre_affaires} £")
    print(f"Panier moyen       : {panier_moyen} £")
    print(f"Lignes valides     : {nb_valides}")

    return {
        'nb_commandes': nb_commandes,
        'nb_clients': nb_clients,
        'chiffre_affaires': chiffre_affaires,
        'panier_moyen': panier_moyen,
        'top_products': top_products,
        'region_metrics': regions
    }

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# ============================================================
# PARTIE 6 : DYNAMIC TASKS PAR RÉGION
# ============================================================
def analyse_region(region, **context):
    filepath = '/opt/airflow/data/dataset.csv'
    nb_transactions = 0
    ca = 0.0
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Country'] == region:
                try:
                    q = float(row['Quantity'])
                    p = float(row['UnitPrice'])
                    if q > 0:
                        ca += q * p
                        nb_transactions += 1
                except ValueError:
                    continue
    ca = round(ca, 2)
    print(f"{region} → {nb_transactions} transactions, CA={ca} £")

def get_regions():
    filepath = '/opt/airflow/data/dataset.csv'
    if not os.path.exists(filepath):
        return ['Unknown']
    regions = set()
    with open(filepath) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Country']:
                regions.add(row['Country'])
    return list(regions)

taches_regions = []
for region in get_regions():
    t = PythonOperator(
        task_id=f'analyse_{region.lower().replace(" ", "_").replace("-", "_")}',
        python_callable=analyse_region,
        op_kwargs={'region': region},
        dag=dag
    )
    taches_regions.append(t)

# ============================================================
# PARTIE 7 : RAPPORT FINAL
# ============================================================
def generate_report(**context):
    kpi = context['ti'].xcom_pull(task_ids='load_data')
    quality = context['ti'].xcom_pull(task_ids='quality_check')

    print("=" * 60)
    print("RAPPORT FINAL — Online Retail Dataset UCI")
    print("=" * 60)
    print(f"Nb commandes         : {kpi['nb_commandes']}")
    print(f"Nb clients           : {kpi['nb_clients']}")
    print(f"Chiffre d'affaires   : {kpi['chiffre_affaires']} £")
    print(f"Panier moyen         : {kpi['panier_moyen']} £")
    print(f"Lignes valides       : {quality['valid_rows']}")
    print(f"Lignes invalides     : {quality['invalid_rows']}")
    print("Top 10 produits :")
    for p, r in kpi['top_products']:
        print(f"  {p[:40]} → {round(r, 2)} £")
    print("=" * 60)

    with open('/opt/airflow/data/rapport_final.csv', 'w') as f:
        f.write("indicateur,valeur\n")
        f.write(f"nb_commandes,{kpi['nb_commandes']}\n")
        f.write(f"nb_clients,{kpi['nb_clients']}\n")
        f.write(f"chiffre_affaires,{kpi['chiffre_affaires']}\n")
        f.write(f"panier_moyen,{kpi['panier_moyen']}\n")
        f.write(f"lignes_valides,{quality['valid_rows']}\n")
        f.write(f"lignes_invalides,{quality['invalid_rows']}\n")

    print("Rapport sauvegardé dans /opt/airflow/data/rapport_final.csv")

task_report = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

# ============================================================
# PARTIE 8 : STOCKAGE MONGODB
# ============================================================
def store_mongodb(**context):
    from pymongo import MongoClient

    kpi = context['ti'].xcom_pull(task_ids='load_data')
    quality = context['ti'].xcom_pull(task_ids='quality_check')

    client = MongoClient('mongodb://ecommerce_mongodb:27017/')
    db = client['ecommerce_analytics']
    collection = db['sales_metrics']

    document = {
        'execution_date': str(context['execution_date']),
        'dag_id': context['dag'].dag_id,
        'dataset': 'online_retail_uci',
        'source_file': 'dataset.csv',
        'status': 'success',
        'global_metrics': {
            'nb_commandes': kpi['nb_commandes'],
            'nb_clients': kpi['nb_clients'],
            'chiffre_affaires': kpi['chiffre_affaires'],
            'panier_moyen': kpi['panier_moyen']
        },
        'top_products': [
            {'product': p, 'revenue': round(r, 2)} for p, r in kpi['top_products']
        ],
        'region_metrics': [
            {'region': r, 'revenue': round(v, 2)} for r, v in kpi['region_metrics'].items()
        ],
        'quality': {
            'valid_rows': quality['valid_rows'],
            'invalid_rows': quality['invalid_rows'],
            'error_file': quality['error_file']
        }
    }

    collection.insert_one(document)
    print(f"Document inséré dans MongoDB : ecommerce_analytics.sales_metrics")
    client.close()

task_mongodb = PythonOperator(
    task_id='store_mongodb',
    python_callable=store_mongodb,
    provide_context=True,
    dag=dag
)

# ============================================================
# DÉPENDANCES
# ============================================================
wait_for_file >> task_validate >> task_quality >> task_branch
task_branch >> task_load
task_branch >> task_stop
task_load >> taches_regions
taches_regions >> task_report
task_report >> task_mongodb