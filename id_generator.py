import hashlib

# Deterministic ID Generator
# Same input → Same ID (Enterprise idempotency)

def generate_deterministic_id(prefix, seed):
    hash_object = hashlib.md5(seed.encode())
    return f"{prefix}-{hash_object.hexdigest()[:8]}"
