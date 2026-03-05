import os
import psycopg2

# Database connection setup
conn = psycopg2.connect(
    dbname="apdireports",
    user="postgres",
    password="d4s31n@",
    host="192.168.2.152",
    port="5432"
)
cursor = conn.cursor()

# 📂 Network Folder Path (UNC format)
image_folder = r"\\mpc2\Users\Public\Merchandise Pictures"

# Function to insert network image paths and extract names
def store_image_paths():
    for root, dirs, files in os.walk(image_folder):
        for file in files:
            if file.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp")):
                image_path = os.path.join(root, file).replace("\\", "/")  # ✅ Convert to forward slashes
                image_name = os.path.splitext(file)[0]  # ✅ Extract file name without extension

                # 🔹 Check if image path already exists in DB
                cursor.execute("SELECT COUNT(*) FROM product_images WHERE path = %s", (image_path,))
                exists = cursor.fetchone()[0]

                if exists == 0:
                    # ✅ Insert new image name and path into DB
                    cursor.execute("INSERT INTO product_images (name, path) VALUES (%s, %s)", (image_name, image_path))
                    print(f"Inserted: {image_name} -> {image_path}")

    conn.commit()
    print("✅ Image names and paths stored successfully.")

# Run function
store_image_paths()

# Close connection
cursor.close()
conn.close()