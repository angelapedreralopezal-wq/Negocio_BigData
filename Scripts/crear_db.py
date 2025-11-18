"""
Script para crear las bases de datos 'presupuesto_db' en MariaDB, MySQL y PostgreSQL
con las tablas definidas en el diagrama de 10 tablas.
"""
import mysql.connector
import psycopg2

# DEFINICIÓN DE TABLAS
TABLAS = {
    "Departamentos": """
        CREATE TABLE Departamentos (
            id_departamento INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100) NOT NULL,
            responsable VARCHAR(100),
            presupuesto_asignado DECIMAL(10,2)
        )
    """,
    "Proveedores": """
        CREATE TABLE Proveedores (
            id_proveedor INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100) NOT NULL,
            contacto VARCHAR(100),
            telefono VARCHAR(20),
            email VARCHAR(100),
            direccion VARCHAR(255)
        )
    """,
    "CategoriasGasto": """
        CREATE TABLE CategoriasGasto (
            id_categoria INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100) NOT NULL,
            descripcion TEXT
        )
    """,
    "Facturas": """
        CREATE TABLE Facturas (
            id_factura INT PRIMARY KEY AUTO_INCREMENT,
            numero_factura VARCHAR(50),
            fecha_emision DATE,
            total DECIMAL(10,2),
            id_proveedor INT,
            id_departamento INT,
            id_categoria INT,
            FOREIGN KEY (id_proveedor) REFERENCES Proveedores(id_proveedor),
            FOREIGN KEY (id_departamento) REFERENCES Departamentos(id_departamento),
            FOREIGN KEY (id_categoria) REFERENCES CategoriasGasto(id_categoria)
        )
    """,
    "Pagos": """
        CREATE TABLE Pagos (
            id_pago INT PRIMARY KEY AUTO_INCREMENT,
            id_factura INT,
            fecha_pago DATE,
            metodo_pago VARCHAR(50),
            monto_pagado DECIMAL(10,2),
            estado VARCHAR(50),
            FOREIGN KEY (id_factura) REFERENCES Facturas(id_factura)
        )
    """,
    "Compras": """
        CREATE TABLE Compras (
            id_compra INT PRIMARY KEY AUTO_INCREMENT,
            id_departamento INT,
            id_proveedor INT,
            descripcion TEXT,
            fecha_compra DATE,
            monto DECIMAL(10,2),
            FOREIGN KEY (id_departamento) REFERENCES Departamentos(id_departamento),
            FOREIGN KEY (id_proveedor) REFERENCES Proveedores(id_proveedor)
        )
    """,
    "Presupuestos": """
        CREATE TABLE Presupuestos (
            id_presupuesto INT PRIMARY KEY AUTO_INCREMENT,
            id_departamento INT,
            anio INT,
            monto_asignado DECIMAL(10,2),
            monto_utilizado DECIMAL(10,2),
            FOREIGN KEY (id_departamento) REFERENCES Departamentos(id_departamento)
        )
    """,
    "Mantenimientos": """
        CREATE TABLE Mantenimientos (
            id_mantenimiento INT PRIMARY KEY AUTO_INCREMENT,
            id_departamento INT,
            descripcion TEXT,
            fecha_inicio DATE,
            fecha_fin DATE,
            costo DECIMAL(10,2),
            FOREIGN KEY (id_departamento) REFERENCES Departamentos(id_departamento)
        )
    """,
    "Usuarios": """
        CREATE TABLE Usuarios (
            id_usuario INT PRIMARY KEY AUTO_INCREMENT,
            nombre VARCHAR(100) NOT NULL,
            rol VARCHAR(50),
            email VARCHAR(100),
            password VARCHAR(255)
        )
    """,
    "AlertasFinancieras": """
        CREATE TABLE AlertasFinancieras (
            id_alerta INT PRIMARY KEY AUTO_INCREMENT,
            tipo VARCHAR(50),
            descripcion TEXT,
            fecha_generada DATE,
            nivel_prioridad VARCHAR(20),
            id_departamento INT,
            FOREIGN KEY (id_departamento) REFERENCES Departamentos(id_departamento)
        )
    """
}

# FUNCIONES PARA CREAR BASES
def crear_mariadb():
    print("Creando base de datos en MariaDB...")
    conexion = mysql.connector.connect(
        host="localhost",
        port=3307,  # revisa el puerto de tu MariaDB
        user="root",
        password="root"
    )
    cursor = conexion.cursor()

    # Eliminar base de datos si existe
    cursor.execute("DROP DATABASE IF EXISTS presupuesto_db;")
    cursor.execute("CREATE DATABASE presupuesto_db;")
    cursor.execute("USE presupuesto_db;")

    # Desactivar comprobación de FK temporalmente
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    for tabla, sql in TABLAS.items():
        cursor.execute(f"DROP TABLE IF EXISTS {tabla};")  # por seguridad
        cursor.execute(sql)
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")  # reactivar FK

    conexion.commit()
    cursor.close()
    conexion.close()
    print("Base de datos MariaDB creada correctamente.\n")


def crear_mysql():
    print("Creando base de datos en MySQL...")
    conexion = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root"
    )
    cursor = conexion.cursor()

    # Eliminar base de datos si existe
    cursor.execute("DROP DATABASE IF EXISTS presupuesto_db;")
    cursor.execute("CREATE DATABASE presupuesto_db;")
    cursor.execute("USE presupuesto_db;")

    # Desactivar FK temporalmente
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
    for tabla, sql in TABLAS.items():
        cursor.execute(f"DROP TABLE IF EXISTS {tabla};")
        cursor.execute(sql)
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")  # reactivar FK

    conexion.commit()
    cursor.close()
    conexion.close()
    print("Base de datos MySQL creada correctamente.\n")


def crear_postgres():
    print("Creando base de datos en PostgreSQL...")
    
    # Conectar a la base 'postgres', no a la que queremos borrar
    conexion = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="postgres"
    )
    conexion.autocommit = True
    cursor = conexion.cursor()

    # Desconectar todos los usuarios de presupuesto_db
    cursor.execute("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = 'presupuesto_db';
    """)

    # Eliminar y crear la base de datos
    cursor.execute("DROP DATABASE IF EXISTS presupuesto_db;")
    cursor.execute("CREATE DATABASE presupuesto_db;")
    cursor.close()
    conexion.close()

    # Conectar a la nueva base de datos
    conexion = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="postgres",
        database="presupuesto_db"
    )
    cursor = conexion.cursor()

    # Ajuste de AUTO_INCREMENT a SERIAL en PostgreSQL
    tablas_pg = {}
    for nombre, sql in TABLAS.items():
        sql_pg = sql.replace("INT PRIMARY KEY AUTO_INCREMENT", "SERIAL PRIMARY KEY")
        tablas_pg[nombre] = sql_pg

    for tabla, sql in tablas_pg.items():
        cursor.execute(f"DROP TABLE IF EXISTS {tabla} CASCADE;")
        cursor.execute(sql)

    conexion.commit()
    cursor.close()
    conexion.close()
    print("Base de datos PostgreSQL creada correctamente.\n")

# EJECUCIÓN
if __name__ == "__main__":
    crear_mariadb()
    crear_mysql()
    crear_postgres()
