"""
Tests for Coolriel
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from datetime import datetime
from os.path import exists
from handlers.user_created_handler import UserCreatedHandler
from handlers.user_deleted_handler import UserDeletedHandler


def test_user_created_handler():
    user_creation = UserCreatedHandler()
    mock_event = {
        "id": 998,
        "name": 'Joanne Test',
        "email": 'joannetest@example.com',
        "datetime": str(datetime.now())
    }
    user_creation.handle(mock_event)
    file_exists = exists("output/welcome_998.html")
    assert file_exists == True

def test_user_deleted_handler():
    user_deletion = UserDeletedHandler()
    mock_event = {
        "id": 999,
        "name": 'Joe Test',
        "email": 'joetest@example.com',
        "datetime": str(datetime.now())
    }
    user_deletion.handle(mock_event)
    file_exists = exists("output/goodbye_999.html")
    assert file_exists == True