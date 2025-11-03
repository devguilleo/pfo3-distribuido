import socket

HOST = "127.0.0.1"
PORT = 5000

def cliente():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"[Cliente] Conectado a {HOST}:{PORT}")

        while True:
            tarea = input("Ingrese una tarea (o 'salir'): ")
            if tarea.lower() == "salir":
                break
            s.sendall(tarea.encode("utf-8"))
            respuesta = s.recv(1024).decode("utf-8")
            print("[Servidor]", respuesta.strip())

if __name__ == "__main__":
    cliente()
