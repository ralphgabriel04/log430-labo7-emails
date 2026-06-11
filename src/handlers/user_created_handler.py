"""
Handler: User Created
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import os
from pathlib import Path
from handlers.base import EventHandler
from typing import Dict, Any

# Correspondance user_type_id -> nom utilisé dans le nom de fichier du template
USER_TYPES = {1: "client", 2: "employee", 3: "manager"}


class UserCreatedHandler(EventHandler):
    """Handles UserCreated events"""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        super().__init__()

    def get_event_type(self) -> str:
        """Return the event type this handler processes"""
        return "UserCreated"

    def _resolve_template(self, user_type_id: int) -> Path:
        """Choisir le template selon le type d'utilisateur (fallback: client)"""
        templates_dir = Path(__file__).parent.parent / "templates"
        type_name = USER_TYPES.get(user_type_id, "client")
        candidate = templates_dir / f"welcome_{type_name}_template.html"
        if not candidate.exists():
            candidate = templates_dir / "welcome_client_template.html"
        return candidate

    def handle(self, event_data: Dict[str, Any]) -> None:
        """Create an HTML email based on user creation data"""

        user_id = event_data.get('id')
        name = event_data.get('name')
        email = event_data.get('email')
        datetime = event_data.get('datetime')
        # Les événements historiques peuvent ne pas avoir de user_type_id -> client par défaut
        user_type_id = event_data.get('user_type_id', 1)

        template_path = self._resolve_template(user_type_id)
        with open(template_path, 'r', encoding='utf-8') as file:
            html_content = file.read()
            html_content = html_content.replace("{{user_id}}", str(user_id))
            html_content = html_content.replace("{{name}}", str(name))
            html_content = html_content.replace("{{email}}", str(email))
            html_content = html_content.replace("{{creation_date}}", str(datetime))

        filename = os.path.join(self.output_dir, f"welcome_{user_id}.html")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(html_content)

        self.logger.debug(f"Courriel HTML (type={USER_TYPES.get(user_type_id, 'client')}) généré à {name} (ID: {user_id}), {filename}")
