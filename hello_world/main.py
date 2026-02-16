import os
from pymongo import MongoClient
from datetime import datetime


def main():
    client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=2000)

    db = client["hello_db"]
    coll = db["hello_collection"]

    data_to_insert = {
        "message": "Первый запуск",
        "created_at": datetime.now()
    }

    try:
        insert_id = coll.insert_one(data_to_insert).inserted_id
        print(f"Вставка успешно выполнена! ID={insert_id}")

        found = coll.find_one({"_id": insert_id})
        print(f"Данные из базы: {found}")
    except Exception as e:
        print(f"Ошибка!\n {e}")


if __name__ == "__main__":
    main()
