from typing import Optional, Any

import pymongo
import sqlite3
import uuid
from datetime import datetime

import config


class Database:
    def __init__(self):
        self.client = pymongo.MongoClient(config.mongodb_uri)
        self.db = self.client["chatgpt_telegram_bot"]

        self.user_collection = self.db["user"]
        self.dialog_collection = self.db["dialog"]

    def check_if_user_exists(self, user_id: int, raise_exception: bool = False):
        if self.user_collection.count_documents({"_id": user_id}) > 0:
            return True
        else:
            if raise_exception:
                raise ValueError(f"User {user_id} does not exist")
            else:
                return False
        
    def add_new_user(
        self,
        user_id: int,
        chat_id: int,
        username: str = "",
        first_name: str = "",
        last_name: str = "",
    ):
        user_dict = {
            "_id": user_id,
            "chat_id": chat_id,

            "username": username,
            "first_name": first_name,
            "last_name": last_name,

            "last_interaction": datetime.now(),
            "first_seen": datetime.now(),
            
            "current_dialog_id": None,
            "current_chat_mode": "assistant",

            "n_used_tokens": 0
        }

        if not self.check_if_user_exists(user_id):
            self.user_collection.insert_one(user_dict)
            
        # TODO: maybe start a new dialog here?

    def start_new_dialog(self, user_id: int):
        self.check_if_user_exists(user_id, raise_exception=True)

        dialog_id = str(uuid.uuid4())
        dialog_dict = {
            "_id": dialog_id,
            "user_id": user_id,
            "chat_mode": self.get_user_attribute(user_id, "current_chat_mode"),
            "start_time": datetime.now(),
            "messages": []
        }

        # add new dialog
        self.dialog_collection.insert_one(dialog_dict)

        # update user's current dialog
        self.user_collection.update_one(
            {"_id": user_id},
            {"$set": {"current_dialog_id": dialog_id}}
        )

        return dialog_id

    def get_user_attribute(self, user_id: int, key: str):
        self.check_if_user_exists(user_id, raise_exception=True)
        user_dict = self.user_collection.find_one({"_id": user_id})

        if key not in user_dict:
            raise ValueError(f"User {user_id} does not have a value for {key}")

        return user_dict[key]

    def set_user_attribute(self, user_id: int, key: str, value: Any):
        self.check_if_user_exists(user_id, raise_exception=True)
        self.user_collection.update_one({"_id": user_id}, {"$set": {key: value}})

    def get_dialog_messages(self, user_id: int, dialog_id: Optional[str] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")

        dialog_dict = self.dialog_collection.find_one({"_id": dialog_id, "user_id": user_id})               
        return dialog_dict["messages"]

    def set_dialog_messages(self, user_id: int, dialog_messages: list, dialog_id: Optional[str] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")
        
        self.dialog_collection.update_one(
            {"_id": dialog_id, "user_id": user_id},
            {"$set": {"messages": dialog_messages}}
        )

class SQLiteDatabase:
    def __init__(self):
        self.conn = sqlite3.connect('chatgpt_telegram_bot.db')
        self.cursor = self.conn.cursor()

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS user
                            (_id INTEGER PRIMARY KEY,
                             chat_id INTEGER,
                             username TEXT,
                             first_name TEXT,
                             last_name TEXT,
                             last_interaction TIMESTAMP,
                             first_seen TIMESTAMP,
                             current_dialog_id TEXT,
                             current_chat_mode TEXT,
                             n_used_tokens INTEGER)''')

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS dialog
                            (_id TEXT PRIMARY KEY,
                             user_id INTEGER,
                             chat_mode TEXT,
                             start_time TIMESTAMP,
                             messages TEXT,
                             FOREIGN KEY(user_id) REFERENCES user(_id))''')

    def check_if_user_exists(self, user_id: int, raise_exception: bool = False):
        self.cursor.execute("SELECT COUNT(*) FROM user WHERE _id=?", (user_id,))
        count = self.cursor.fetchone()[0]
        if count > 0:
            return True
        else:
            if raise_exception:
                raise ValueError(f"User {user_id} does not exist")
            else:
                return False

    def add_new_user(
        self,
        user_id: int,
        chat_id: int,
        username: str = "",
        first_name: str = "",
        last_name: str = "",
    ):
        user_dict = {
            "_id": user_id,
            "chat_id": chat_id,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "last_interaction": datetime.now(),
            "first_seen": datetime.now(),
            "current_dialog_id": None,
            "current_chat_mode": "assistant",
            "n_used_tokens": 0
        }

        if not self.check_if_user_exists(user_id):
            self.cursor.execute("INSERT INTO user VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (user_id, chat_id, username, first_name, last_name,
                                datetime.now(), datetime.now(), None, "assistant", 0))
            self.conn.commit()
            
        # TODO: maybe start a new dialog here?

    def start_new_dialog(self, user_id: int):
        self.check_if_user_exists(user_id, raise_exception=True)

        dialog_id = str(uuid.uuid4())
        dialog_dict = {
            "_id": dialog_id,
            "user_id": user_id,
            "chat_mode": self.get_user_attribute(user_id, "current_chat_mode"),
            "start_time": datetime.now(),
            "messages": []
        }

        # add new dialog
        self.cursor.execute("INSERT INTO dialog VALUES (?, ?, ?, ?, ?)",
                            (dialog_id, user_id, self.get_user_attribute(user_id, "current_chat_mode"),
                            datetime.now(), ""))
        self.conn.commit()

        # update user's current dialog
        self.cursor.execute("UPDATE user SET current_dialog_id=? WHERE _id=?", (dialog_id, user_id))
        self.conn.commit()

        return dialog_id

    def get_user_attribute(self, user_id: int, key: str):
        self.check_if_user_exists(user_id, raise_exception=True)
        self.cursor.execute("SELECT * FROM user WHERE _id=?", (user_id,))
        user_dict = dict(self.cursor.fetchone())

        if key not in user_dict:
            raise ValueError(f"User {user_id} does not have a value for {key}")

        return user_dict[key]

    def set_user_attribute(self, user_id: int, key: str, value: Any):
        self.check_if_user_exists(user_id, raise_exception=True)
        self.cursor.execute(f"UPDATE user SET {key}=? WHERE _id=?", (value, user_id))
        self.conn.commit()

    def get_dialog_messages(self, user_id: int, dialog_id: Optional[str] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")

        self.cursor.execute("SELECT messages FROM dialog WHERE _id=? AND user_id=?", (dialog_id, user_id))
        dialog_dict = self.cursor.fetchone()

        return dialog_dict[0]

    def set_dialog_messages(self, user_id: int, dialog_messages: list, dialog_id: Optional[str] = None):
        self.check_if_user_exists(user_id, raise_exception=True)

        if dialog_id is None:
            dialog_id = self.get_user_attribute(user_id, "current_dialog_id")
        
        self.cursor.execute("UPDATE dialog SET messages=? WHERE _id=? AND user_id=?", 
                            (dialog_messages, dialog_id, user_id))
        self.conn.commit()
