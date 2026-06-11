"""
Coolriel: Event-Driven Email Sender
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import config
from consumers.user_event_history_consumer import UserEventHistoryConsumer
from logger import Logger
from consumers.user_event_consumer import UserEventConsumer
from handlers.handler_registry import HandlerRegistry
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler

logger = Logger.get_instance("Coolriel")

def main():
    """Main entry point for the Coolriel service"""
    registry = HandlerRegistry()
    registry.register(UserCreatedHandler(output_dir=config.OUTPUT_DIR))
    registry.register(UserDeletedHandler(output_dir=config.OUTPUT_DIR))

    # 1) Event sourcing: lire d'abord TOUT l'historique des événements (earliest).
    # group_id distinct + consumer_timeout_ms => ce consommateur se termine,
    # puis l'exécution continue vers le consommateur "live".
    consumer_service_history = UserEventHistoryConsumer(
        bootstrap_servers=config.KAFKA_HOST,
        topic=config.KAFKA_TOPIC,
        group_id=f"{config.KAFKA_GROUP_ID}-history",
        output_dir=config.OUTPUT_DIR,
        consumer_timeout_ms=5000,
    )
    history = consumer_service_history.start()
    logger.info(f"Historique terminé : {len(history)} événement(s) récupéré(s).")

    # 2) Consommateur "live": écoute les nouveaux événements (latest).
    # NOTE: le consommateur peut écouter 1 ou plusieurs topics (str or array)
    consumer_service = UserEventConsumer(
        bootstrap_servers=config.KAFKA_HOST,
        topic=config.KAFKA_TOPIC,
        group_id=config.KAFKA_GROUP_ID,
        registry=registry,
    )
    consumer_service.start()

if __name__ == "__main__":
    main()
