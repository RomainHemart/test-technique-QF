# Test Technique - Quantile CA Clients

Programme Go pour l'analyse du chiffre d'affaires clients par quantiles, avec export vers MySQL.

## 📋 Description

Ce programme suit l'architecture BigData standard : **Load → Compute in Memory → Export**

**Objectif** : Identifier les meilleurs clients (top quantile) en fonction de leur chiffre d'affaires généré depuis une date donnée.

### Fonctionnalités

- ✅ Lecture des données depuis MySQL (sans JOINs)
- ✅ Calcul du CA par client (EventTypeID = 6)
- ✅ Analyse par quantiles configurable (défaut: 2.5%)
- ✅ Export des top clients vers table MySQL avec mass insert
- ✅ Logging structuré avec progress bars
- ✅ Gestion détaillée des prix manquants

## 🚀 Installation

### Prérequis

- Go 1.25.1 ou supérieur
- MySQL 5.7+ ou MariaDB
- Accès à la base de données e-commerce

### Installation des dépendances

```bash
go mod download
```

## ⚙️ Configuration

### Variables d'environnement

Configurez les accès à la base de données :

```bash
export DB_USER="candidat2020"
export DB_PASS="dfskj_878$*="
export DB_HOST="44.333.11.22"
export DB_PORT="3306"
export DB_NAME="ecommerce"
```

**Note** : Si DB_HOST ou DB_PORT ne sont pas définis, les valeurs par défaut sont `127.0.0.1:3306`

### Mode verbose (optionnel)

```bash
export VERBOSE="true"
```

## 📊 Utilisation

### Commande de base

```bash
go run main.go -quantile=0.025 -since=2020-04-01
```

### Options disponibles

| Option      | Type    | Défaut       | Description                                    |
|-------------|---------|--------------|------------------------------------------------|
| `-quantile` | float64 | 0.025        | Fraction du quantile (ex: 0.025 = 2.5%)        |
| `-since`    | string  | 2020-04-01   | Date de début pour les événements (YYYY-MM-DD) |
| `-v`        | bool    | false        | Active le mode verbose                         |

### Exemples

**Analyse avec quantiles de 5%**
```bash
go run main.go -quantile=0.05 -since=2020-01-01 -v
```

**Analyse depuis le début 2021**
```bash
go run main.go -since=2021-01-01
```

**Mode debug complet**
```bash
go run main.go -quantile=0.025 -since=2020-04-01 -v
```

## 🏗️ Architecture

### Schéma de base de données

```
Customer ────┐
             ├──── CustomerEventData ───── Content ──── ContentPrice
CustomerData┘                              
```

### Phases du traitement

#### 1. LOAD (Chargement)
- `CustomerEventData` : Événements d'achat (EventTypeID = 6, EventDate >= since)
- `ContentPrice` : Prix des produits (garde le plus récent par ContentID)
- `CustomerData` : Emails clients (ChannelTypeID = 1, garde le plus récent)

#### 2. COMPUTE (Calcul en mémoire)
- Construction des maps de prix et emails
- Calcul du CA par client : `CA = Σ(Quantity × Price)`
- Tri décroissant par CA
- Calcul des quantiles et extraction du top quantile

#### 3. EXPORT (Sauvegarde)
- Création de la table `test_export_YYYYMMDD`
- Mass insert par batches de 500 lignes
- UPDATE si CustomerID existe déjà

## 📈 Exemple de sortie

```
INFO[2025-10-04T10:15:30+02:00] starting process. quantile=0.025 since=2020-04-01  stage=START
INFO[2025-10-04T10:15:31+02:00] loading events                                stage=LOAD table=CustomerEventData
INFO[2025-10-04T10:15:35+02:00] events loaded                                 loaded_events=125643
INFO[2025-10-04T10:15:35+02:00] loading content prices                        stage=LOAD
INFO[2025-10-04T10:15:36+02:00] content prices loaded                         loaded_prices=5420
INFO[2025-10-04T10:15:36+02:00] loading customer emails (CustomerData channel=1)  stage=LOAD
INFO[2025-10-04T10:15:38+02:00] customer emails loaded                        loaded_emails=23456
INFO[2025-10-04T10:15:38+02:00] price map built                               price_map_size=5420
INFO[2025-10-04T10:15:38+02:00] email map built                               email_map_size=23456
computing CA 100% |████████████████████████████████████████| (125643/125643, 45321 it/s)
WARN[2025-10-04T10:15:41+02:00] missing prices detected                       percentage_skipped=2.34% total_events_skipped=2940 unique_content_ids=142
INFO[2025-10-04T10:15:41+02:00] computed CA per customer                      customers_with_ca=18234
INFO[2025-10-04T10:15:41+02:00] printing 10 random samples from CA map (total=18234)
INFO[2025-10-04T10:15:41+02:00] sample 1: CustomerID=12345 CA=5423.67
INFO[2025-10-04T10:15:41+02:00] sample 2: CustomerID=67890 CA=1234.50
...
INFO[2025-10-04T10:15:41+02:00] ========== QUANTILE ANALYSIS ==========
INFO[2025-10-04T10:15:41+02:00] quantile summary                              avg_ca=8542.18 max_ca=15234.87 min_ca=1849.48 nb_clients=456 quantile_index=0 quantile_range="0.0% - 2.5%"
INFO[2025-10-04T10:15:41+02:00] quantile summary                              avg_ca=1245.34 max_ca=1849.47 min_ca=641.21 nb_clients=456 quantile_index=1 quantile_range="2.5% - 5.0%"
...
INFO[2025-10-04T10:15:41+02:00] =======================================
INFO[2025-10-04T10:15:41+02:00] top quantile extracted                        top_quantile_size=456
INFO[2025-10-04T10:15:41+02:00] exporting top customers (batch)               count=456 stage=EXPORT table=test_export_20251004
exporting batches 100% |████████████████████████████████████████| (456/456, 2341 it/s)
INFO[2025-10-04T10:15:42+02:00] process finished                              duration=11.234s
```

## 📦 Structure du projet

```
.
├── main.go           # Programme principal
├── go.mod            # Dépendances Go
├── go.sum            # Checksums des dépendances
└── README.md         # Ce fichier
```

## 🔍 Détails techniques

### Calcul du chiffre d'affaires

Le CA pour un client est calculé ainsi :
```
CA_client = Σ (Quantity × Price)
```
Pour tous les EventData où :
- `EventTypeID = 6` (Purchase)
- `EventDate >= since`
- `Price` provient de la ligne ContentPrice la plus récente pour chaque ContentID

### Gestion des prix manquants

Si un ContentID n'a pas de prix dans ContentPrice :
- L'événement est **ignoré** pour le calcul du CA
- Un warning détaillé est loggé avec statistiques :
  - Nombre de ContentID uniques sans prix
  - Nombre total d'événements ignorés
  - Pourcentage d'événements ignorés

### Calcul des quantiles

Avec `quantile = 0.025` (2.5%) :
- 40 quantiles sont créés : 0-2.5%, 2.5-5%, ..., 97.5-100%
- Tri décroissant par CA → quantile 0 = top clients
- Taille par quantile : `ceil(nb_clients / 40)`

### Mass insert

Export par batches de 500 lignes avec `ON DUPLICATE KEY UPDATE` :
- Si CustomerID existe → UPDATE Email et CA
- Sinon → INSERT nouvelle ligne

## 🔧 Dépendances

- `github.com/go-sql-driver/mysql` : Driver MySQL
- `github.com/schollz/progressbar/v3` : Barres de progression
- `github.com/sirupsen/logrus` : Logging structuré

## 📝 Table de sortie

Structure de `test_export_YYYYMMDD` :

```sql
CREATE TABLE test_export_20251004 (
    CustomerID BIGINT NOT NULL PRIMARY KEY,
    Email VARCHAR(255),
    CA DECIMAL(18,2) NOT NULL
) ENGINE=InnoDB;
```

### Vérification des résultats

```sql
-- Nombre de clients exportés
SELECT COUNT(*) FROM test_export_20251004;

-- Statistiques CA
SELECT 
    COUNT(*) as nb_clients,
    MIN(CA) as ca_min,
    MAX(CA) as ca_max,
    AVG(CA) as ca_moyen
FROM test_export_20251004;

-- Top 10 clients
SELECT CustomerID, Email, CA 
FROM test_export_20251004 
ORDER BY CA DESC 
LIMIT 10;
```

---

### Formule de calcul détaillée

Pour un client donné :

```
CA = Σ (event.Quantity × price.Price)
```

Où :
- `event ∈ CustomerEventData`
- `event.CustomerID = client.CustomerID`
- `event.EventTypeID = 6` (Purchase)
- `event.EventDate >= since` (paramètre -since)
- `price = ContentPrice` le plus récent pour `event.ContentID`
- Si `price` n'existe pas → événement ignoré

### Algorithme de quantile

Avec N clients et quantile q :

1. **Nombre de quantiles** : `nb_quantiles = round(1/q)`
   - Exemple : q=0.025 → 40 quantiles

2. **Taille par quantile** : `size = ceil(N / nb_quantiles)`
   - Exemple : 18234 clients → ceil(18234/40) = 456 clients/quantile

3. **Distribution** :
   - Quantile 0 (top) : clients[0:456] (CA les plus élevés)
   - Quantile 1 : clients[456:912]
   - ...
   - Quantile 39 (bottom) : clients[17784:18234] (CA les plus faibles)

4. **Statistiques par quantile** :
   - `min_ca` = CA du dernier client du quantile
   - `max_ca` = CA du premier client du quantile
   - `nb_clients` = taille effective du quantile

---

**Version du README** : 1.0  
**Dernière mise à jour** : 04/10/2025  