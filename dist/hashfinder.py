import hashlib, os
with open("xDrive-0.7.zip", "rb") as f:
    h = hashlib.sha256(f.read()).hexdigest()
print("Hash:", h)
print("Size:", os.path.getsize("xDrive-0.7.zip"))
