"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
import json
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer


class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        output_dir: str = "output",
        consumer_timeout_ms: int = 5000,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.output_dir = output_dir
        self.consumer_timeout_ms = consumer_timeout_ms
        # Lire depuis le tout début de la partition (event sourcing)
        self.auto_offset_reset = "earliest"
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
        os.makedirs(self.output_dir, exist_ok=True)

    def start(self) -> None:
        """Read the full event history from the topic and save it to disk"""
        self.logger.info(f"Démarrer un consommateur historique : {self.group_id}")

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            enable_auto_commit=False,
            # Le consommateur s'arrête X ms après le dernier événement historique
            consumer_timeout_ms=self.consumer_timeout_ms,
        )

        events = []
        try:
            # consumer_timeout_ms fait sortir la boucle après le dernier message
            for message in self.consumer:
                events.append(message.value)
                self.logger.debug(
                    f"Historique - offset {message.offset}: {message.value.get('event')} "
                    f"(id={message.value.get('id')})"
                )

            # IMPORTANT: écriture par lots APRÈS la boucle (1 seule opération I/O)
            output_path = os.path.join(self.output_dir, "user_event_history.json")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(events, f, ensure_ascii=False, indent=2)

            self.logger.info(
                f"{len(events)} événement(s) historique(s) enregistré(s) dans {output_path}"
            )
        except Exception as e:
            self.logger.error(f"Erreur: {e}", exc_info=True)
        finally:
            self.stop()

        return events

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur historique!")
