# Rapport — Labo 07 : Architecture Event-Driven, Event Sourcing et Pub/Sub

**LOG430-02 — Architecture logicielle, École de technologie supérieure (ÉTS)**

| | |
|---|---|
| **Chargé de laboratoire** | Gabriel C. Ullmann |
| **Étudiant** | Ralph Christian Gabriel |
| **Code permanent** | GABR77340401 |
| **Session** | Été 2026 |
| **Application** | Store Manager (suite du Labo 03) |
| **Date des mesures** | 2026-06-10 |

---

## Contexte

Ce laboratoire intègre le microservice **Coolriel** (génération de courriels HTML) au
**Store Manager** (labo 5) via **Apache Kafka**. Le Store Manager *produit* des événements
(`UserCreated`, `UserDeleted`) sur le topic `user-events`; Coolriel les *consomme* et génère
les courriels correspondants, sans jamais appeler directement le Store Manager.

Architecture validée de bout en bout (voir captures de logs en annexe) :

```
[Store Manager] --produce--> (Kafka: user-events) --consume--> [Coolriel] --> fichiers .html
   (producteur)                  (broker, 7j)                    (consommateur)
```

---

## Question 1 — Différence entre `store_manager↔coolriel` (labo 7) et `store_manager↔payments_api` (labo 5)

### Labo 5 : communication **synchrone**, point-à-point (REST via KrakenD)

Au labo 5, le Store Manager appelle `payments_api` par une **requête HTTP REST** (via la
passerelle KrakenD). C'est un couplage **temporel et spatial** : l'appelant connaît l'URL du
service appelé, attend la réponse, et bloque tant qu'elle n'arrive pas.

```
store_manager  --- POST /payments-api/payments --->  api-gateway (KrakenD)  --->  payments_api
               <---------------- 200 OK / 503 ---------------------------------------
```

- L'émetteur **connaît** le destinataire (`http://payments_api:5009`, défini dans `krakend.json`).
- L'émetteur **attend** une réponse (requête/réponse, bloquant, `timeout: 5s`).
- Si `payments_api` est **indisponible**, l'appel **échoue** immédiatement (couplage fort).

### Labo 7 : communication **asynchrone**, publish/subscribe (Kafka)

Au labo 7, le Store Manager **publie** un événement sur Kafka et continue **sans attendre**.
Il ne connaît pas Coolriel; il connaît seulement le **topic** `user-events`.

```python
# store_manager — src/orders/commands/write_user.py (producteur)
user_event_producer = UserEventProducer()
user_event_producer.get_instance().send('user-events', value={
    'event': 'UserCreated',
    'id': new_user.id, 'name': new_user.name, 'email': new_user.email,
    'user_type_id': new_user.user_type_id,
    'datetime': str(datetime.datetime.now())
})
```

```python
# coolriel — src/consumers/user_event_consumer.py (consommateur)
self.consumer = KafkaConsumer('user-events', bootstrap_servers=..., group_id=...,
                              value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in self.consumer:
    self._process_message(message.value)   # -> handler -> génère le HTML
```

- L'émetteur **ne connaît pas** le destinataire (découplage spatial) : Coolriel pourrait être
  remplacé/dupliqué sans changer le Store Manager.
- L'émetteur **n'attend pas** (découplage temporel) : la réponse HTTP `/users` revient même si
  Coolriel est arrêté. L'événement reste dans Kafka jusqu'à 7 jours et sera traité plus tard.
- On peut ajouter **N consommateurs** (SMS, analytics, …) sur le même topic sans toucher au producteur.

### Avantages / Inconvénients

| Critère | Synchrone REST (labo 5) | Asynchrone Kafka (labo 7) |
|---|---|---|
| Couplage | Fort (URL + temps) | Faible (topic seulement) |
| Disponibilité | Si le service appelé tombe, l'appel échoue | Le producteur continue; l'événement est rejoué plus tard |
| Latence perçue par le client | Inclut le temps du service appelé | Quasi nulle (fire-and-forget) |
| Réponse immédiate / cohérence | Forte cohérence (on a la réponse) | Cohérence **éventuelle** |
| Ajout de consommateurs | Modifier l'appelant | Aucun changement (s'abonner au topic) |
| Débogage / traçage | Simple (1 appel = 1 réponse) | Plus complexe (flux d'événements) |
| Historique | Aucun par défaut | **Event sourcing** possible (rétention Kafka) |

**Conclusion :** le REST synchrone convient quand on a besoin de la réponse immédiatement
(ex. : confirmer un paiement avant de finaliser une commande). Le pub/sub Kafka convient aux
**effets de bord non bloquants** (envoyer un courriel) où le découplage et la résilience priment.

---

## Question 2 — Méthodes modifiées dans `src/orders/commands/write_user.py`

J'ai modifié les **deux** méthodes : `add_user` et `delete_user`.

**`add_user`** — ajout du paramètre `user_type_id`, persistance de celui-ci, et émission de
l'événement `UserCreated` (incluant `user_type_id`) :

```python
def add_user(name: str, email: str, user_type_id: int = 1):
    ...
    new_user = User(name=name, email=email, user_type_id=user_type_id)
    session.add(new_user); session.flush(); session.commit()
    UserEventProducer().get_instance().send('user-events', value={
        'event': 'UserCreated', 'id': new_user.id, 'name': new_user.name,
        'email': new_user.email, 'user_type_id': new_user.user_type_id,
        'datetime': str(datetime.datetime.now())})
    return new_user.id
```

**`delete_user`** — **capture des données AVANT la suppression** (sinon le nom/courriel/type
sont perdus et le courriel d'au revoir ne peut être généré), puis émission de `UserDeleted` :

```python
def delete_user(user_id: int):
    ...
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        deleted_user = {'id': user.id, 'name': user.name,
                        'email': user.email, 'user_type_id': user.user_type_id}
        session.delete(user); session.commit()
        UserEventProducer().get_instance().send('user-events', value={
            'event': 'UserDeleted', **deleted_user,
            'datetime': str(datetime.datetime.now())})
        return 1
    return 0
```

Modifications connexes : `user_controller.create_user` lit `user_type_id` du corps de la requête;
le modèle `User` reçoit la colonne `user_type_id`; `db-init/init.sql` ajoute la table `user_types`
et la `FOREIGN KEY`.

---

## Question 3 — Vérification du type d'utilisateur dans Coolriel

Le type voyage **dans l'événement** (`user_type_id`). Côté Coolriel, chaque handler fait
correspondre l'id à un nom et **choisit dynamiquement le template** (avec repli sur `client`
pour rester rétro-compatible avec d'anciens événements sans `user_type_id`) :

```python
# handlers/user_created_handler.py (idem dans user_deleted_handler.py)
USER_TYPES = {1: "client", 2: "employee", 3: "manager"}

def _resolve_template(self, user_type_id: int) -> Path:
    templates_dir = Path(__file__).parent.parent / "templates"
    type_name = USER_TYPES.get(user_type_id, "client")          # vérification du type
    candidate = templates_dir / f"welcome_{type_name}_template.html"
    if not candidate.exists():
        candidate = templates_dir / "welcome_client_template.html"  # fallback
    return candidate

def handle(self, event_data):
    user_type_id = event_data.get('user_type_id', 1)   # défaut: client
    template_path = self._resolve_template(user_type_id)
    ...
```

Ainsi :
- un **employé** (`user_type_id = 2`) reçoit *« 👋 Salut et bienvenue dans l'équipe ! »*,
- un **directeur** (`3`) reçoit le message de la direction,
- un **client** (`1`) reçoit *« 🎉 Bienvenu·e »*.

Le message d'au revoir est personnalisé de la même façon (`goodbye_{type}_template.html`).
Validé dans les logs : `type=client` / `type=employee` / `type=manager` selon le cas.

---

## Question 4 — Partitionnement Kafka et performances de lecture

Résumé des points principaux (documentation officielle Kafka — *Persistence / Efficiency*) :

1. **Append-only + I/O séquentielles.** Chaque partition est un *log* immuable où les messages
   sont écrits/lus **séquentiellement**. Les disques sont très rapides en accès séquentiel
   (vs aléatoire), ce qui rend lectures et écritures efficaces même sur disque dur.
2. **Page cache de l'OS.** Kafka s'appuie sur le cache de pages du système plutôt que sur un
   cache applicatif en mémoire JVM : les lectures récentes sont servies depuis la RAM, sans
   surcoût de GC ni double mise en cache.
3. **Parallélisme par partitions.** Un topic est divisé en **partitions** réparties sur les
   brokers. La lecture/écriture se fait en parallèle sur plusieurs partitions → le débit
   **scale horizontalement** avec le nombre de partitions et de brokers.
4. **Groupes de consommateurs.** Au sein d'un `group_id`, Kafka **répartit les partitions**
   entre les consommateurs (au plus 1 consommateur par partition). On augmente le débit de
   lecture en ajoutant des consommateurs jusqu'au nombre de partitions.
5. **Offsets côté consommateur.** Le broker ne suit pas l'état de chaque message; chaque
   consommateur gère son **offset**. Le broker reste *stateless* et léger → lectures rapides
   et possibilité de **rejouer** l'historique (utile pour l'event sourcing).
6. **Zero-copy (`sendfile`).** Pour livrer les messages, Kafka copie directement du page cache
   vers le socket réseau (zero-copy), évitant des copies mémoire et passages user/kernel inutiles.
7. **Traitement par lots + compression.** Les messages sont groupés en *batches* (et compressés),
   réduisant le nombre d'I/O et de requêtes réseau.

**En une phrase :** Kafka atteint un haut débit de lecture en combinant *logs séquentiels +
page cache + zero-copy* (efficacité par partition) et *partitionnement + groupes de
consommateurs* (parallélisme horizontal).

---

## Question 5 — Nombre d'événements récupérés par le consommateur historique

Le `UserEventHistoryConsumer` lit depuis le **début** (`auto_offset_reset="earliest"`), avec un
**`group_id` distinct** (`coolriel-group-history`) et un **`consumer_timeout_ms=5000`** pour
s'arrêter après le dernier message historique. L'écriture se fait **par lots** (une seule
opération I/O après la boucle) au format JSON.

- Lors du premier test manuel (3 créations + 3 suppressions) : **6 événements** récupérés.
- Après le test de charge Locust : **1103 événements** récupérés (preuve que la rétention Kafka
  conserve bien l'historique).

Extrait de `output/user_event_history.json` :

```json
[
  { "event": "UserCreated", "id": 6, "name": "Grace Hopper",
    "email": "ghopper@example.com", "user_type_id": 1,
    "datetime": "2026-06-11 21:29:23.509258" },
  { "event": "UserCreated", "id": 7, "name": "New Employee",
    "email": "newemp@magasinducoin.ca", "user_type_id": 2,
    "datetime": "2026-06-11 21:29:23.740381" },
  { "event": "UserDeleted", "id": 1, "name": "Ada Lovelace",
    "email": "alovelace@example.com", "user_type_id": 1,
    "datetime": "2026-06-11 21:29:23.972067" }
]
```

Log : `UserEventHistoryConsumer - INFO - 1103 événement(s) historique(s) enregistré(s) dans output/user_event_history.json`

---

## Activité 8 — Test de charge Locust (observations)

**Cible :** `store_manager:5000` directement (la passerelle KrakenD n'expose pas `DELETE /users`).
**Scénario :** 75 % crée+supprime un utilisateur, 25 % crée seulement (courriels uniques pour
respecter la contrainte `UNIQUE` sur `email`).
**Charge :** 30 utilisateurs simultanés, *spawn* 5/s, durée 45 s.

| Endpoint | # req | Échecs | Moy (ms) | Méd (ms) | p95 (ms) | Max (ms) |
|---|---|---|---|---|---|---|
| POST /users | 474 | 0 | 77 | 68 | 150 | 272 |
| POST /users (create only) | 147 | 0 | 83 | 71 | 170 | 260 |
| DELETE /users/[id] | 474 | 0 | 70 | 61 | 130 | 230 |
| **Agrégé** | **1095** | **0 (0 %)** | **75** | **65** | **150** | **272** |

Débit : **≈ 24,5 req/s**, **0 échec**.

**Observations :**
- **Aucun échec** : la production Kafka est **asynchrone** (`send()` non bloquant), donc la
  génération d'événements n'ajoute pas de latence visible aux réponses HTTP. Le client n'attend
  pas Coolriel.
- Latences faibles et stables (médiane 65 ms, p95 150 ms) : le goulot reste MySQL
  (INSERT/DELETE + contrainte `UNIQUE`/FK), pas Kafka.
- Le `KafkaProducer` est un **Singleton** : une seule connexion réutilisée sur toutes les
  requêtes, ce qui évite le coût de reconnexion sous charge.
- Tous les événements générés ont bien été **persistés** dans Kafka et relus ensuite par le
  consommateur historique (1103 événements au total), confirmant la rétention.

---

## Annexe — Validation (logs Coolriel)

```
UserEventConsumer - DEBUG - Evenement : UserCreated
Handler - DEBUG - Courriel HTML (type=client)   généré à Grace Hopper (ID: 6),  output/welcome_6.html
Handler - DEBUG - Courriel HTML (type=employee) généré à New Employee (ID: 7),  output/welcome_7.html
Handler - DEBUG - Courriel HTML (type=manager)  généré à New Boss (ID: 8),      output/welcome_8.html
Handler - DEBUG - Courriel d'au revoir (type=client)   généré à Ada Lovelace (ID: 1), output/goodbye_1.html
Handler - DEBUG - Courriel d'au revoir (type=employee) généré à Jane Doe (ID: 4),     output/goodbye_4.html
Handler - DEBUG - Courriel d'au revoir (type=manager)  généré à Da Boss (ID: 5),      output/goodbye_5.html
```
