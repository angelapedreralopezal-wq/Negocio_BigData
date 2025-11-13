"""
Script para rellenar la base de datos 'presupuesto_db' usando Faker.
Se utilizan 10 providers distintos de Faker:

1. name           → para nombres de personas
2. company        → para nombres de proveedores y empresas
3. address        → para direcciones
4. phone_number   → para teléfonos
5. email          → para correos electrónicos
6. date_between   → para fechas de emisión, pagos, mantenimiento, etc.
7. job            → para roles de usuarios
8. text           → para descripciones
9. random_int     → para cantidades, IDs aleatorios
10. random_element → para seleccionar valores de listas (método de pago, estado, prioridad)
"""

from faker import Faker
import mysql.connector
import random
from datetime import date, timedelta

# ------------------------------
# Conexión a la base de datos MySQL
# ------------------------------
conexion = mysql.connector.connect(
    host="localhost",
    port=3306,              
    user="root",
    password="root",
    database="presupuesto_db"
)

cursor = conexion.cursor()
fake = Faker('es_ES')  # Faker configurado en español

# ------------------------------
# Funciones de inserción
# ------------------------------

def insertar_departamentos(n=5):
    for _ in range(n):
        nombre = fake.company()
        responsable = fake.name()
        presupuesto = round(random.uniform(50000, 200000), 2)
        cursor.execute("""
            INSERT INTO Departamentos (nombre, responsable, presupuesto_asignado)
            VALUES (%s, %s, %s)
        """, (nombre, responsable, presupuesto))
    conexion.commit()

def insertar_proveedores(n=10):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO Proveedores (nombre, contacto, telefono, email, direccion)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fake.company(),
            fake.name(),
            fake.phone_number(),
            fake.email(),
            fake.address()
        ))
    conexion.commit()

def insertar_categorias(n=6):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO CategoriasGasto (nombre, descripcion)
            VALUES (%s, %s)
        """, (fake.word().capitalize(), fake.text(max_nb_chars=100)))
    conexion.commit()

def insertar_usuarios(n=5):
    for _ in range(n):
        cursor.execute("""
            INSERT INTO Usuarios (nombre, rol, email, password)
            VALUES (%s, %s, %s, %s)
        """, (
            fake.name(),
            fake.job(),
            fake.email(),
            fake.password(length=10)
        ))
    conexion.commit()

def insertar_presupuestos():
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [row[0] for row in cursor.fetchall()]
    for dept in departamentos:
        for anio in range(2023, 2026):
            monto_asignado = round(random.uniform(50000, 150000), 2)
            monto_utilizado = round(monto_asignado * random.uniform(0.5, 0.9), 2)
            cursor.execute("""
                INSERT INTO Presupuestos (id_departamento, anio, monto_asignado, monto_utilizado)
                VALUES (%s, %s, %s, %s)
            """, (dept, anio, monto_asignado, monto_utilizado))
    conexion.commit()

def insertar_facturas(n=20):
    cursor.execute("SELECT id_proveedor FROM Proveedores")
    proveedores = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id_categoria FROM CategoriasGasto")
    categorias = [row[0] for row in cursor.fetchall()]

    for _ in range(n):
        cursor.execute("""
            INSERT INTO Facturas (numero_factura, fecha_emision, total, id_proveedor, id_departamento, id_categoria)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            f"FAC-{fake.random_int(1000,9999)}",
            fake.date_between(start_date='-1y', end_date='today'),
            round(random.uniform(500, 5000), 2),
            random.choice(proveedores),
            random.choice(departamentos),
            random.choice(categorias)
        ))
    conexion.commit()

def insertar_pagos():
    cursor.execute("SELECT id_factura FROM Facturas")
    facturas = [row[0] for row in cursor.fetchall()]
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

def insertar_compras(n=15):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT id_proveedor FROM Proveedores")
    proveedores = [row[0] for row in cursor.fetchall()]

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

def insertar_mantenimientos(n=10):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [row[0] for row in cursor.fetchall()]

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

def insertar_alertas(n=10):
    cursor.execute("SELECT id_departamento FROM Departamentos")
    departamentos = [row[0] for row in cursor.fetchall()]
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

# ------------------------------
# Ejecución del llenado
# ------------------------------
insertar_departamentos()
insertar_proveedores()
insertar_categorias()
insertar_usuarios()
insertar_presupuestos()
insertar_facturas()
insertar_pagos()
insertar_compras()
insertar_mantenimientos()
insertar_alertas()

print("Base de datos 'presupuesto_db' rellenada con datos falsos correctamente.")

# Cerrar conexión
cursor.close()
conexion.close()
