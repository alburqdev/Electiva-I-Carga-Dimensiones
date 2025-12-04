CREATE DATABASE IF NOT EXISTS DW_Ventas;
USE DW_Ventas;

DROP TABLE IF EXISTS DimCliente;

CREATE TABLE DimCliente (
    ClienteKey INT AUTO_INCREMENT PRIMARY KEY,
    ClienteID INT NOT NULL,
    NombreCliente VARCHAR(100),
    Apellido VARCHAR(100),
    Email VARCHAR(150),
    Telefono VARCHAR(20),
    Direccion VARCHAR(200),
    Ciudad VARCHAR(100),
    Pais VARCHAR(100),
    CodigoPostal VARCHAR(20),
    FechaRegistro DATE,
    Estado VARCHAR(20),
    -- Campos de auditoría
    FechaCarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_DimCliente_ClienteID ON DimCliente(ClienteID);
CREATE INDEX IX_DimCliente_Ciudad ON DimCliente(Ciudad);

DROP TABLE IF EXISTS DimCategoria;

CREATE TABLE DimCategoria (
    CategoriaKey INT AUTO_INCREMENT PRIMARY KEY,
    CategoriaID INT NOT NULL,
    NombreCategoria VARCHAR(100) NOT NULL,
    Descripcion VARCHAR(500),
    -- Campos de auditoría
    FechaCarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_DimCategoria_CategoriaID ON DimCategoria(CategoriaID);

DROP TABLE IF EXISTS DimProducto;

CREATE TABLE DimProducto (
    ProductoKey INT AUTO_INCREMENT PRIMARY KEY,
    ProductoID INT NOT NULL,
    CategoriaKey INT,
    NombreProducto VARCHAR(200) NOT NULL,
    Descripcion VARCHAR(500),
    Precio DECIMAL(10,2),
    Stock INT,
    Marca VARCHAR(100),
    Proveedor VARCHAR(150),
    UnidadMedida VARCHAR(50),
    Estado VARCHAR(20),
    -- Campos de auditoría
    FechaCarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (CategoriaKey) REFERENCES DimCategoria(CategoriaKey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_DimProducto_ProductoID ON DimProducto(ProductoID);
CREATE INDEX IX_DimProducto_CategoriaKey ON DimProducto(CategoriaKey);
CREATE INDEX IX_DimProducto_Estado ON DimProducto(Estado);

DROP TABLE IF EXISTS DimAlmacen;

CREATE TABLE DimAlmacen (
    AlmacenKey INT AUTO_INCREMENT PRIMARY KEY,
    AlmacenID INT NOT NULL,
    NombreAlmacen VARCHAR(100) NOT NULL,
    Direccion VARCHAR(200),
    Ciudad VARCHAR(100),
    Pais VARCHAR(100),
    CodigoPostal VARCHAR(20),
    Capacidad INT,
    Responsable VARCHAR(100),
    Telefono VARCHAR(20),
    Estado VARCHAR(20),
    -- Campos de auditoría
    FechaCarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    FechaActualizacion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_DimAlmacen_AlmacenID ON DimAlmacen(AlmacenID);
CREATE INDEX IX_DimAlmacen_Ciudad ON DimAlmacen(Ciudad);

DROP TABLE IF EXISTS DimFecha;

CREATE TABLE DimFecha (
    FechaKey INT PRIMARY KEY,
    Fecha DATE NOT NULL,
    Anio INT NOT NULL,
    Mes INT NOT NULL,
    NombreMes VARCHAR(20) NOT NULL,
    Trimestre INT NOT NULL,
    Semestre INT NOT NULL,
    DiaMes INT NOT NULL,
    DiaSemana INT NOT NULL,
    NombreDiaSemana VARCHAR(20) NOT NULL,
    SemanaMes INT NOT NULL,
    SemanaAnio INT NOT NULL,
    EsFinDeSemana TINYINT(1) NOT NULL,
    EsFeriado TINYINT(1) NOT NULL,
    NombreFeriado VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_DimFecha_Fecha ON DimFecha(Fecha);
CREATE INDEX IX_DimFecha_AnioMes ON DimFecha(Anio, Mes);

DROP TABLE IF EXISTS FactVentas;

CREATE TABLE FactVentas (
    VentaKey INT AUTO_INCREMENT PRIMARY KEY,
    FechaKey INT NOT NULL,
    ClienteKey INT NOT NULL,
    ProductoKey INT NOT NULL,
    AlmacenKey INT NOT NULL,
    OrderID INT NOT NULL,
    OrderDetailID INT,
    Cantidad INT NOT NULL,
    PrecioUnitario DECIMAL(10,2) NOT NULL,
    Descuento DECIMAL(5,2) DEFAULT 0,
    MontoTotal DECIMAL(12,2) NOT NULL,
    CostoTotal DECIMAL(12,2),
    Ganancia DECIMAL(12,2),
    -- Campos de auditoría
    FechaCarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (FechaKey) REFERENCES DimFecha(FechaKey),
    FOREIGN KEY (ClienteKey) REFERENCES DimCliente(ClienteKey),
    FOREIGN KEY (ProductoKey) REFERENCES DimProducto(ProductoKey),
    FOREIGN KEY (AlmacenKey) REFERENCES DimAlmacen(AlmacenKey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX IX_FactVentas_FechaKey ON FactVentas(FechaKey);
CREATE INDEX IX_FactVentas_ClienteKey ON FactVentas(ClienteKey);
CREATE INDEX IX_FactVentas_ProductoKey ON FactVentas(ProductoKey);
CREATE INDEX IX_FactVentas_AlmacenKey ON FactVentas(AlmacenKey);
CREATE INDEX IX_FactVentas_OrderID ON FactVentas(OrderID);

SELECT 'Data Warehouse creado exitosamente' AS Mensaje;

SELECT * FROM dimalmacen