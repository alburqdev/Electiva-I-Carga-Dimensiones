import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime, timedelta
import re
import numpy as np

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_ventas.log'),
        logging.StreamHandler()
    ]
)

class VentasDataWarehouseETL:
    def __init__(self, host, database, username, password, port=3306):
        """Inicializar conexión a MySQL"""
        try:
            self.conn = mysql.connector.connect(
                host="127.0.0.1",
                database="DW_Ventas",
                user="root",
                password="Beliusky128@",
                port=3306
            )
            logging.info("Conexión exitosa a la base de datos MySQL")
        except Error as e:
            logging.error(f"Error al conectar a MySQL: {str(e)}")
            raise

    def limpiar_telefono(self, telefono):
        """Limpiar y estandarizar números de teléfono"""
        if pd.isna(telefono) or telefono == '':
            return None
        
        # Convertir a string y limpiar
        telefono_str = str(telefono)
        
        # Remover caracteres no numéricos excepto + al inicio
        telefono_limpio = re.sub(r'[^\d+]', '', telefono_str)
        
        # Si tiene notación científica (E), convertir
        if 'E' in str(telefono).upper() or 'e' in str(telefono):
            try:
                telefono_limpio = str(int(float(telefono)))
            except:
                return None
        
        return telefono_limpio if telefono_limpio else None

    def limpiar_precio(self, precio):
        if pd.isna(precio):
            return 0.0
        try:
            # Convertir a float, manejando puntos como decimales
            return float(precio)
        except:
            return 0.0

    def limpiar_fecha(self, fecha):
        if pd.isna(fecha) or fecha == '' or str(fecha).strip() == '#' or str(fecha).strip().startswith('#'):
            return None
        
        try:
            # Intentar parsear la fecha
            return pd.to_datetime(fecha, errors='coerce')
        except:
            return None

    def cargar_dim_categoria(self, ruta_csv):
        logging.info("Iniciando carga de DimCategoria...")
        
        try:
            # Leer archivo de productos
            df_productos = pd.read_csv(ruta_csv)
            
            # Extraer categorías únicas
            categorias_unicas = df_productos['Category'].dropna().unique()
            
            # Crear DataFrame de categorías
            df_categorias = pd.DataFrame({
                'CategoriaID': range(1, len(categorias_unicas) + 1),
                'NombreCategoria': categorias_unicas,
                'Descripcion': [f'Categoría de {cat}' for cat in categorias_unicas]
            })
            
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            cursor.execute("DELETE FROM DimCategoria")
            cursor.execute("ALTER TABLE DimCategoria AUTO_INCREMENT = 1")
            
            # Insertar categorías
            for _, row in df_categorias.iterrows():
                cursor.execute("""
                    INSERT INTO DimCategoria (CategoriaID, NombreCategoria, Descripcion)
                    VALUES (%s, %s, %s)
                """, (row['CategoriaID'], row['NombreCategoria'], row['Descripcion']))
            
            self.conn.commit()
            logging.info(f"✓ DimCategoria cargada: {len(df_categorias)} registros")
            
            return df_categorias
            
        except Exception as e:
            logging.error(f"Error en DimCategoria: {str(e)}")
            self.conn.rollback()
            raise

    def cargar_dim_cliente(self, ruta_csv):
        logging.info("Iniciando carga de DimCliente...")
        
        try:
            df = pd.read_csv(ruta_csv)
            
            # Limpiar datos
            df['Phone'] = df['Phone'].apply(self.limpiar_telefono)
            df['Email'] = df['Email'].fillna('')
            df['City'] = df['City'].fillna('Desconocido')
            df['Country'] = df['Country'].fillna('Desconocido')
            
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            cursor.execute("DELETE FROM DimCliente")
            cursor.execute("ALTER TABLE DimCliente AUTO_INCREMENT = 1")
            
            # Insertar clientes
            registros_cargados = 0
            for _, row in df.iterrows():
                try:
                    cursor.execute("""
                        INSERT INTO DimCliente 
                        (ClienteID, NombreCliente, Apellido, Email, Telefono, 
                         Ciudad, Pais, Estado)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['CustomerID'], 
                        row['FirstName'], 
                        row['LastName'],
                        row['Email'],
                        row['Phone'],
                        row['City'],
                        row['Country'],
                        'Activo'
                    ))
                    registros_cargados += 1
                except Exception as e:
                    logging.warning(f"Error en cliente {row['CustomerID']}: {str(e)}")
            
            self.conn.commit()
            logging.info(f"✓ DimCliente cargada: {registros_cargados} registros")
            
        except Exception as e:
            logging.error(f"Error en DimCliente: {str(e)}")
            self.conn.rollback()
            raise

    def cargar_dim_producto(self, ruta_csv, df_categorias):
        logging.info("Iniciando carga de DimProducto...")
        
        try:
            df = pd.read_csv(ruta_csv)
            
            # Limpiar precios
            df['Price'] = df['Price'].apply(self.limpiar_precio)
            df['Stock'] = df['Stock'].fillna(0).astype(int)
            
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            cursor.execute("DELETE FROM DimProducto")
            cursor.execute("ALTER TABLE DimProducto AUTO_INCREMENT = 1")
            
            # Insertar productos
            registros_cargados = 0
            for _, row in df.iterrows():
                try:
                    # Buscar CategoriaKey
                    categoria_key = None
                    if pd.notna(row['Category']):
                        cat_row = df_categorias[df_categorias['NombreCategoria'] == row['Category']]
                        if not cat_row.empty:
                            cursor.execute(
                                "SELECT CategoriaKey FROM DimCategoria WHERE CategoriaID = %s",
                                (int(cat_row.iloc[0]['CategoriaID']),)
                            )
                            result = cursor.fetchone()
                            if result:
                                categoria_key = result[0]
                    
                    cursor.execute("""
                        INSERT INTO DimProducto 
                        (ProductoID, CategoriaKey, NombreProducto, Precio, Stock, Estado)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        row['ProductID'],
                        categoria_key,
                        row['ProductName'],
                        row['Price'],
                        row['Stock'],
                        'Activo'
                    ))
                    registros_cargados += 1
                except Exception as e:
                    logging.warning(f"Error en producto {row['ProductID']}: {str(e)}")
            
            self.conn.commit()
            logging.info(f"✓ DimProducto cargada: {registros_cargados} registros")
            
        except Exception as e:
            logging.error(f"Error en DimProducto: {str(e)}")
            self.conn.rollback()
            raise

    def cargar_dim_almacen(self):
        logging.info("Iniciando carga de DimAlmacen...")
        
        try:
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            cursor.execute("DELETE FROM DimAlmacen")
            cursor.execute("ALTER TABLE DimAlmacen AUTO_INCREMENT = 1")
            
            # Crear almacén genérico
            cursor.execute("""
                INSERT INTO DimAlmacen 
                (AlmacenID, NombreAlmacen, Ciudad, Pais, Estado)
                VALUES (%s, %s, %s, %s, %s)
            """, (1, 'Almacén Central', 'Ciudad Principal', 'País', 'Activo'))
            
            self.conn.commit()
            logging.info("✓ DimAlmacen cargada: 1 registro (genérico)")
            
        except Exception as e:
            logging.error(f"Error en DimAlmacen: {str(e)}")
            self.conn.rollback()
            raise

    def cargar_dim_fecha(self, ruta_csv, anios_extra=2):
        logging.info("Iniciando carga de DimFecha...")
        
        try:
            # Leer fechas de orders
            df_orders = pd.read_csv(ruta_csv)
            df_orders['OrderDate'] = df_orders['OrderDate'].apply(self.limpiar_fecha)
            
            # Filtrar fechas válidas
            fechas_validas = df_orders['OrderDate'].dropna()
            
            if len(fechas_validas) == 0:
                logging.warning("No se encontraron fechas válidas. Generando fechas por defecto.")
                fecha_inicio = datetime(2020, 1, 1)
                fecha_fin = datetime.now() + timedelta(days=365)
            else:
                fecha_inicio = fechas_validas.min()
                fecha_fin = fechas_validas.max() + timedelta(days=365 * anios_extra)
            
            # Generar rango de fechas
            fechas = pd.date_range(start=fecha_inicio, end=fecha_fin, freq='D')
            
            df_fechas = pd.DataFrame({
                'Fecha': fechas,
                'FechaKey': fechas.strftime('%Y%m%d').astype(int),
                'Anio': fechas.year,
                'Mes': fechas.month,
                'NombreMes': fechas.strftime('%B'),
                'Trimestre': fechas.quarter,
                'Semestre': ((fechas.month - 1) // 6) + 1,
                'DiaMes': fechas.day,
                'DiaSemana': fechas.dayofweek + 1,
                'NombreDiaSemana': fechas.strftime('%A'),
                'SemanaMes': fechas.to_series().apply(lambda x: (x.day - 1) // 7 + 1).values,
                'SemanaAnio': fechas.isocalendar().week,
                'EsFinDeSemana': (fechas.dayofweek >= 5).astype(int),
                'EsFeriado': 0
            })
            
            cursor = self.conn.cursor()
            
            # Limpiar tabla
            cursor.execute("DELETE FROM DimFecha")
            
            # Insertar fechas
            for _, row in df_fechas.iterrows():
                cursor.execute("""
                    INSERT INTO DimFecha 
                    (FechaKey, Fecha, Anio, Mes, NombreMes, Trimestre, Semestre,
                     DiaMes, DiaSemana, NombreDiaSemana, SemanaMes, SemanaAnio,
                     EsFinDeSemana, EsFeriado)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['FechaKey'], row['Fecha'], row['Anio'], row['Mes'],
                    row['NombreMes'], row['Trimestre'], row['Semestre'],
                    row['DiaMes'], row['DiaSemana'], row['NombreDiaSemana'],
                    row['SemanaMes'], row['SemanaAnio'], row['EsFinDeSemana'],
                    row['EsFeriado']
                ))
            
            self.conn.commit()
            logging.info(f"✓ DimFecha cargada: {len(df_fechas)} registros")
            
        except Exception as e:
            logging.error(f"Error en DimFecha: {str(e)}")
            self.conn.rollback()
            raise

    def ejecutar_etl_completo(self, ruta_customers, ruta_products, ruta_orders):
        """Ejecutar el proceso ETL completo"""
        logging.info("=" * 60)
        logging.info("INICIANDO PROCESO ETL - DATA WAREHOUSE VENTAS")
        logging.info("=" * 60)
        
        try:
            # 1. Cargar DimCategoria (primero, porque DimProducto depende de ella)
            df_categorias = self.cargar_dim_categoria(ruta_products)
            
            # 2. Cargar DimCliente
            self.cargar_dim_cliente(ruta_customers)
            
            # 3. Cargar DimProducto
            self.cargar_dim_producto(ruta_products, df_categorias)
            
            # 4. Cargar DimAlmacen
            self.cargar_dim_almacen()
            
            # 5. Cargar DimFecha
            self.cargar_dim_fecha(ruta_orders)
            
            logging.info("=" * 60)
            logging.info("✓ PROCESO ETL COMPLETADO EXITOSAMENTE")
            logging.info("=" * 60)
            
        except Exception as e:
            logging.error(f"Error en el proceso ETL: {str(e)}")
            raise
        finally:
            self.conn.close()
            logging.info("Conexión cerrada")

# EJECUCIÓN DEL ETL
if __name__ == "__main__":
    # Configuración de conexión MySQL
    HOST = 'localhost'
    DATABASE = 'DW_Ventas'
    USERNAME = 'tu_usuario'
    PASSWORD = 'tu_password'
    PORT = 3306
    
    # Rutas de archivos CSV
    RUTA_CUSTOMERS = 'customers.csv'
    RUTA_PRODUCTS = 'products.csv'
    RUTA_ORDERS = 'orders.csv'
    
    try:
        # Crear instancia del ETL
        etl = VentasDataWarehouseETL(HOST, DATABASE, USERNAME, PASSWORD, PORT)
        
        # Ejecutar ETL completo
        etl.ejecutar_etl_completo(RUTA_CUSTOMERS, RUTA_PRODUCTS, RUTA_ORDERS)
        
        print("\n✓ Proceso completado. Revisa el archivo 'etl_ventas.log' para más detalles.")
        
    except Exception as e:
        print(f"\n✗ Error en el proceso: {str(e)}")
        print("Revisa el archivo 'etl_ventas.log' para más detalles.")