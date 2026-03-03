from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

with open("private.pem", "rb") as f:
    key_data = f.read()

password = b"YourStrongPasswordHere"  # EXACTLY your passphrase

key = serialization.load_pem_private_key(
    key_data,
    password=password,
    backend=default_backend(),
)

print("Loaded OK:", key)
