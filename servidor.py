import socket
import threading
import queue
import time
import sqlite3
import os

# Configuración
HOST = "127.0.0.1"
PORT = 5000
NUM_WORKERS = 3

# Cola de tareas
tareas = queue.Queue()

# Crear bases de datos por worker (simulando almacenamiento distribuido)
def inicializar_bases():
    for i in range(NUM_WORKERS):
        db_name = f"worker{i+1}.db"
        if not os.path.exists(db_name):
            conn = sqlite3.connect(db_name)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resultados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    tarea TEXT,
                    resultado TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            conn.close()

# Función ejecutada por cada worker
def worker_func(id_worker):
    db_name = f"worker{id_worker}.db"
    print(f"[Worker {id_worker}] Iniciado y esperando tareas...")

    while True:
        tarea = tareas.get()
        if tarea is None:  # señal de finalización
            print(f"[Worker {id_worker}] Finalizando...")
            break

        print(f"[Worker {id_worker}] Procesando tarea: {tarea}")
        time.sleep(2)  # simula tiempo de trabajo
        resultado = f"Tarea '{tarea}' procesada por Worker {id_worker}"

        # Guardar en base del worker
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO resultados (tarea, resultado) VALUES (?, ?)", (tarea, resultado))
        conn.commit()
        conn.close()

        print(f"[Worker {id_worker}] Guardó resultado en {db_name}")
        tareas.task_done()

# Manejar clientes
def manejar_cliente(conn, addr):
    print(f"[Servidor] Cliente conectado: {addr}")
    try:
        while True:
            data = conn.recv(1024)
            if not data:
                break
            tarea = data.decode("utf-8").strip()
            print(f"[Servidor] Recibida tarea: {tarea}")
            tareas.put(tarea)
            conn.sendall(b"Tarea recibida y encolada\n")
    except ConnectionResetError:
        print(f"[Servidor] Cliente desconectado abruptamente: {addr}")
    finally:
        conn.close()
        print(f"[Servidor] Cliente {addr} desconectado")

# Servidor principal
def iniciar_servidor():
    inicializar_bases()

    # Crear threads de workers
    for i in range(NUM_WORKERS):
        threading.Thread(target=worker_func, args=(i+1,), daemon=True).start()

    # Crear socket del servidor
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        print(f"[Servidor] Escuchando en {HOST}:{PORT} con {NUM_WORKERS} workers")

        try:
            while True:
                conn, addr = s.accept()
                threading.Thread(target=manejar_cliente, args=(conn, addr)).start()
        except KeyboardInterrupt:
            print("\n[Servidor] Finalizando...")

        # Enviar señal de parada a los workers
        for _ in range(NUM_WORKERS):
            tareas.put(None)
        tareas.join()

if __name__ == "__main__":
    iniciar_servidor()
