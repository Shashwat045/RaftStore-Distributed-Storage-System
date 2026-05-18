import os
import json
import bcrypt
import jwt
from datetime import datetime, timedelta

# In a real system, this would be a secure environment variable
SECRET_KEY = "raftstore_super_secret_key"
ALGORITHM = "HS256"
AUTH_DB_PATH = "auth_db.json"

def load_users():
    if os.path.exists(AUTH_DB_PATH):
        with open(AUTH_DB_PATH, "r") as f:
            return json.load(f)
    return {}

def save_users(users):
    with open(AUTH_DB_PATH, "w") as f:
        json.dump(users, f, indent=4)

def hash_password(password: str) -> str:
    # bcrypt.hashpw returns bytes, we decode to string for JSON storage
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False

def create_jwt_token(email: str) -> str:
    expiration = datetime.utcnow() + timedelta(hours=24)
    payload = {
        "sub": email,
        "exp": expiration
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_jwt_token(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None
