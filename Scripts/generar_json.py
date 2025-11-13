import pymysql
import mysql.connector
import psycopg2
import json
from datetime import date

# ------------------------------
# Funciones para generar JSON
# ------------------------------

def generar_json_mariadb():
    # Conexión MariaDB con pymysql
    conexion = pymysql.connect(
        host="localhost",
        user="root",
        password="root",
        database="presupuesto_db",
        charset='utf8mb4',
        port=3307
    )
    cursor = conexion.cursor(pymysql.cursors.DictCursor)

    # Consulta 1: gasto por departamento y año
    cursor.execute("""
        SELECT d.nombre AS departamento, p.anio, 
               SUM(p.monto_asignado) AS presupuesto_asignado,
               SUM(p.monto_utilizado) AS presupuesto_utilizado
        FROM Presupuestos p
        JOIN Departamentos d ON p.id_departamento = d.id_departamento
        GROUP BY d.nombre, p.anio
        ORDER BY p.anio, d.nombre;
    """)
    gasto_departamento = cursor.fetchall()

    # Consulta 2: facturas pendientes o retrasadas
    cursor.execute("""
        SELECT f.numero_factura, f.total, d.nombre AS departamento, pr.nombre AS proveedor, pg.estado
        FROM Facturas f
        JOIN Pagos pg ON f.id_factura = pg.id_factura
        JOIN Departamentos d ON f.id_departamento = d.id_departamento
        JOIN Proveedores pr ON f.id_proveedor = pr.id_proveedor
        WHERE pg.estado IN ('Pendiente', 'Retrasado')
        ORDER BY f.numero_factura;
    """)
    facturas_retrasadas = cursor.fetchall()

    # Guardar JSON
    datos = {
        "gasto_departamento": gasto_departamento,
        "facturas_retrasadas": facturas_retrasadas
    }

    with open("mariadb_analisis.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, indent=4, default=str)

    cursor.close()
    conexion.close()
    print("JSON de MariaDB generado: mariadb_analisis.json")

# ------------------------------
# MySQL
# ------------------------------
def generar_json_mysql():
    conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="presupuesto_db"
    )
    cursor = conexion.cursor(dictionary=True)

    # Consulta 1: alertas recientes por departamento
    cursor.execute("""
        SELECT a.tipo, a.descripcion, a.nivel_prioridad, a.fecha_generada, d.nombre AS departamento
        FROM AlertasFinancieras a
        JOIN Departamentos d ON a.id_departamento = d.id_departamento
        WHERE a.fecha_generada >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
        ORDER BY a.nivel_prioridad DESC;
    """)
    alertas_recientes = cursor.fetchall()

    # Consulta 2: proveedores con compras mayores
    cursor.execute("""
        SELECT pr.nombre AS proveedor, d.nombre AS departamento, c.monto, c.fecha_compra
        FROM Compras c
        JOIN Proveedores pr ON c.id_proveedor = pr.id_proveedor
        JOIN Departamentos d ON c.id_departamento = d.id_departamento
        WHERE c.monto > 5000
        ORDER BY c.monto DESC;
    """)
    compras_grandes = cursor.fetchall()

    datos = {
        "alertas_recientes": alertas_recientes,
        "compras_grandes": compras_grandes
    }

    with open("mysql_analisis.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, indent=4, default=str)

    cursor.close()
    conexion.close()
    print("JSON de MySQL generado: mysql_analisis.json")

# ------------------------------
# PostgreSQL
# ------------------------------
def generar_json_postgres():
    conexion = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="presupuesto_db"
    )
    cursor = conexion.cursor()

    # Consulta 1: resumen de pagos por método
    cursor.execute("""
        SELECT metodo_pago, COUNT(*) AS num_pagos, SUM(monto_pagado) AS total_pagado
        FROM Pagos
        GROUP BY metodo_pago
        ORDER BY total_pagado DESC;
    """)
    pagos_metodo = [
        {"metodo_pago": row[0], "num_pagos": row[1], "total_pagado": float(row[2])}
        for row in cursor.fetchall()
    ]

    # Consulta 2: mantenimientos recientes con costo elevado
    cursor.execute("""
        SELECT m.descripcion, m.costo, d.nombre AS departamento, m.fecha_inicio, m.fecha_fin
        FROM Mantenimientos m
        JOIN Departamentos d ON m.id_departamento = d.id_departamento
        WHERE m.costo > 5000
        ORDER BY m.costo DESC;
    """)
    mantenimientos_costosos = [
        {"descripcion": row[0], "costo": float(row[1]), "departamento": row[2], "fecha_inicio": str(row[3]), "fecha_fin": str(row[4])}
        for row in cursor.fetchall()
    ]

    datos = {
        "pagos_metodo": pagos_metodo,
        "mantenimientos_costosos": mantenimientos_costosos
    }

    with open("postgres_analisis.json", "w", encoding="utf-8") as f:
        json.dump(datos, f, indent=4, default=str)

    cursor.close()
    conexion.close()
    print("JSON de PostgreSQL generado: postgres_analisis.json")


# ------------------------------
# EJECUCIÓN
# ------------------------------
if __name__ == "__main__":
    generar_json_mariadb()
    generar_json_mysql()
    generar_json_postgres()
