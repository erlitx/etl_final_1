from pymongo import MongoClient
import os
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@mongo:27017")
DB_NAME = os.getenv("MONGO_DB")
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

fake = Faker()

def get_count(var_name, default):
    return int(os.getenv(var_name, default))

users = [str(uuid.uuid4()) for _ in range(get_count("USER_COUNT", 1000))]
products = [str(uuid.uuid4()) for _ in range(get_count("PRODUCT_COUNT", 500))]

def generate_data(generator, count):
    return generator(count)

def generate_user_sessions(n):
    return [{
        "session_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "start_time": (start_time := fake.date_time_this_year()).isoformat(),
        "end_time": (start_time + timedelta(minutes=random.randint(5, 120))).isoformat(),
        "pages_visited": [fake.uri_path() for _ in range(random.randint(1, 10))],
        "device": fake.user_agent(),
        "actions": [fake.word() for _ in range(random.randint(1, 5))]
    } for _ in range(n)]

def generate_product_price_history(n):
    return [{
        "product_id": random.choice(products),
        "price_changes": [{
            "date": (datetime.now() - timedelta(days=i)).isoformat(),
            "price": round(random.uniform(10, 1000), 2)
        } for i in range(random.randint(1, 10))],
        "current_price": round(random.uniform(10, 1000), 2),
        "currency": "USD"
    } for _ in range(n)]

def generate_event_logs(n):
    event_types = ["login", "logout", "purchase", "error", "click"]
    return [{
        "event_id": str(uuid.uuid4()),
        "timestamp": fake.date_time_this_year().isoformat(),
        "event_type": random.choice(event_types),
        "details": fake.sentence()
    } for _ in range(n)]

def generate_support_tickets(n):
    statuses = ["open", "closed", "pending"]
    issues = ["login issue", "payment failure", "bug report", "feature request"]
    return [{
        "ticket_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "status": random.choice(statuses),
        "issue_type": random.choice(issues),
        "messages": [fake.sentence() for _ in range(random.randint(1, 5))],
        "created_at": fake.date_time_this_year().isoformat(),
        "updated_at": fake.date_time_this_year().isoformat()
    } for _ in range(n)]

def generate_user_recommendations(n):
    return [{
        "user_id": random.choice(users),
        "recommended_products": [random.choice(products) for _ in range(random.randint(1, 5))],
        "last_updated": fake.date_time_this_year().isoformat()
    } for _ in range(n)]

def generate_moderation_queue(n):
    statuses = ["pending", "approved", "rejected"]
    return [{
        "review_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "product_id": random.choice(products),
        "review_text": fake.text(),
        "rating": random.randint(1, 5),
        "moderation_status": random.choice(statuses),
        "flags": [fake.word() for _ in range(random.randint(0, 3))],
        "submitted_at": fake.date_time_this_year().isoformat()
    } for _ in range(n)]

def generate_search_queries(n):
    return [{
        "query_id": str(uuid.uuid4()),
        "user_id": random.choice(users),
        "query_text": fake.sentence(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "filters": [fake.word() for _ in range(random.randint(0, 3))],
        "results_count": random.randint(0, 50)
    } for _ in range(n)]

def insert_data(collection, generator, count):
    data = generate_data(generator, count)
    db[collection].insert_many(data)
    print(f"{collection.replace('_', ' ').capitalize()} загружено в MongoDB: {count}")


# Генерируем

insert_data("user_sessions", generate_user_sessions, get_count("USER_SESSIONS_COUNT", 1000))
insert_data("product_price_history", generate_product_price_history, get_count("PRODUCT_PRICE_HISTORY_COUNT", 1000))
insert_data("event_logs", generate_event_logs, get_count("EVENT_LOGS_COUNT", 2000))
insert_data("support_tickets", generate_support_tickets, get_count("SUPPORT_TICKETS_COUNT", 500))
insert_data("user_recommendations", generate_user_recommendations, get_count("USER_RECOMMENDATIONS_COUNT", 1000))
insert_data("moderation_queue", generate_moderation_queue, get_count("MODERATION_QUEUE_COUNT", 500))
insert_data("search_queries", generate_search_queries, get_count("SEARCH_QUERIES_COUNT", 1000))
