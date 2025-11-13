"""
Script unificado para rellenar la base de datos 'presupuesto_db' usando Faker.
Compatible con MySQL, MariaDB y PostgreSQL.
Se rellenan las tres bases de datos automáticamente.
"""

from faker import Faker
import random
from datetime import timedelta

# Configuración de las bases de datos
dbs = {
    "mysql": {
        "module": "mysql.connector",
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "root",
        "database": "presupuesto_db"
    },
    "mariadb": {
        "module": "pymysql",
        "host": "localhost",
        "port": 3307,
        "user": "root",
        "password": "root",
        "database": "presupuesto_db",
        "charset": "utf8mb4"
    },
    "postgres": {
        "module": "psycopg2",
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "presupuesto_db"
    }
}

fake = Faker('es_ES')

# --- Funciones de inserción (idénticas para todas las DBs) ---
def insertar_departamentos(cursor, conexion, n=5):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO Departamentos (nombre, responsable, presupuesto_asignado)
            VALUES (%s, %s, %s)
        """, (fake.company(), fake.name(), round(random.uniform(50000, 200000), 2)))
    conexion.commit()

def insertar_proveedores(cursor, conexion, n=10):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO Proveedores (nombre, contacto, telefono, email, direccion)
            VALUES (%s, %s, %s, %s, %s)
        """, (fake.company(), fake.name(), fake.phone_number(), fake.email(), fake.address()))
    conexion.commit()

def insertar_categorias(cursor, conexion, n=6):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO CategoriasGasto (nombre, descripcion)
            VALUES (%s, %s)
        """, (fake.word().capitalize(), fake.text(max_nb_chars=100)))
    conexion.commit()

def insertar_usuarios(cursor, conexion, n=5):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO Usuarios (nombre, rol, email, password)
            VALUES (%s, %s, %s, %s)
        """, (fake.name(), fake.job()[:50], fake.email()[:100], fake.password(length=10)))
    conexion.commit()

def insertar_presupuestos(cursor, conexion):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [r[0] for r in cursor.fetchall()]
    for dept in departamentos:
        for anio in range(2023, 2026):
            monto_asignado = round(random.uniform(50000, 150000), 2)
            monto_utilizado = round(monto_asignado * random.uniform(0.5, 0.9), 2)
            cursor.execute("""
                INSERT INTO Presupuestos (id_departamento, anio, monto_asignado, monto_utilizado)
                VALUES (%s, %s, %s, %s)
            """, (dept, anio, monto_asignado, monto_utilizado))
    conexion.commit()

def insertar_facturas(cursor, conexion, n=20):
    cursor.execute("SELECT id_proveedor FROM Proveedores")
    proveedores = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT id_categoria FROM CategoriasGasto")
    categorias = [r[0] for r in cursor.fetchall()]

    for _ in range(n):
        cursor.execute("""
            INSERT INTO Facturas (numero_factura, fecha_emision, total, id_proveedor, id_departamento, id_categoria)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            f"FAC-{fake.random_int(1000, 9999)}",
            fake.date_between(start_date='-1y', end_date='today'),
            round(random.uniform(500, 5000), 2),
            random.choice(proveedores),
            random.choice(departamentos),
            random.choice(categorias)
        ))
    conexion.commit()

def insertar_pagos(cursor, conexion):
    cursor.execute("SELECT id_factura FROM Facturas")
    facturas = [r[0] for r in cursor.fetchall()]
    metodos = ["Transferencia", "Tarjeta", "Efectivo"]
    estados = ["Pagado", "Pendiente", "Retrasado"]

    for factura in facturas:
        cursor.execute("""
            INSERT INTO Pagos (id_factura, fecha_pago, metodo_pago, monto_pagado, estado)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            factura,
            fake.date_between(start_date='-6m', end_date='today'),
            random.choice(metodos),
            round(random.uniform(100, 5000), 2),
            random.choice(estados)
        ))
    conexion.commit()

def insertar_compras(cursor, conexion, n=15):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT id_proveedor FROM Proveedores")
    proveedores = [r[0] for r in cursor.fetchall()]

    for _ in range(n):
        cursor.execute("""
            INSERT INTO Compras (id_departamento, id_proveedor, descripcion, fecha_compra, monto)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            random.choice(departamentos),
            random.choice(proveedores),
            fake.text(max_nb_chars=100),
            fake.date_between(start_date='-1y', end_date='today'),
            round(random.uniform(100, 10000), 2)
        ))
    conexion.commit()

def insertar_mantenimientos(cursor, conexion, n=10):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [r[0] for r in cursor.fetchall()]

    for _ in range(n):
        inicio = fake.date_between(start_date='-1y', end_date='today')
        fin = inicio + timedelta(days=random.randint(1, 15))
        cursor.execute("""
            INSERT INTO Mantenimientos (id_departamento, descripcion, fecha_inicio, fecha_fin, costo)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            random.choice(departamentos),
            fake.text(max_nb_chars=100),
            inicio,
            fin,
            round(random.uniform(1000, 10000), 2)
        ))
    conexion.commit()

def insertar_alertas(cursor, conexion, n=10):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [r[0] for r in cursor.fetchall()]
    prioridades = ["Alta", "Media", "Baja"]

    for _ in range(n):
        cursor.execute("""
            INSERT INTO AlertasFinancieras (tipo, descripcion, fecha_generada, nivel_prioridad, id_departamento)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            random.choice(["Presupuesto excedido", "Pago retrasado", "Factura sin registrar"]),
            fake.text(max_nb_chars=100),
            fake.date_between(start_date='-1m', end_date='today'),
            random.choice(prioridades),
            random.choice(departamentos)
        ))
    conexion.commit()


# --- Llenado de todas las bases de datos ---
for motor, cfg in dbs.items():
    print(f"\nConectando a {motor}...")
    
    if motor in ["mysql", "mariadb"]:
        # Importar dinámicamente el submódulo correcto
        mod_full = __import__(cfg["module"], fromlist=['connect'])
        conexion = mod_full.connect(**{k:v for k,v in cfg.items() if k != "module"})
    else:
        import psycopg2
        conexion = psycopg2.connect(**{k:v for k,v in cfg.items() if k != "module"})
    
    cursor = conexion.cursor()
    print(f"Rellenando datos en {motor}...")

    insertar_departamentos(cursor, conexion)
    insertar_proveedores(cursor, conexion)
    insertar_categorias(cursor, conexion)
    insertar_usuarios(cursor, conexion)
    insertar_presupuestos(cursor, conexion)
    insertar_facturas(cursor, conexion)
    insertar_pagos(cursor, conexion)
    insertar_compras(cursor, conexion)
    insertar_mantenimientos(cursor, conexion)
    insertar_alertas(cursor, conexion)

    print(f"Base de datos '{cfg['database']}' rellenada correctamente en {motor}.")
    cursor.close()
    conexion.close()


print("\n¡Todas las bases de datos han sido rellenadas!")
