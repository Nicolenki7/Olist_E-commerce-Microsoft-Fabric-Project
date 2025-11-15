// ETL_DimSeller.m
// Propósito: Creación de la dimensión 'DimSeller'. Limpia los datos del vendedor y los enriquece con
// coordenadas geográficas (lat/lng) y el nombre completo del estado (estado_completo).
//
// Fuentes de datos:
// - olist_sellers_dataset csv (Tabla base del vendedor)
// - olist_geolocation_dataset csv (Geolocalización para lat/lng)
// - DimState (Tabla auxiliar con la traducción de la sigla del estado al nombre completo)

let
    // 1. CARGA E INICIO DE LA TABLA DIMENSIÓN
    // Se asume que esta es la tabla base 'olist_sellers_dataset'
    Origen_Seller = Lakehouse.Contents(null),
    // [Se omiten pasos de navegación específicos de Fabric (Lakehouse.Contents, Navegación, etc.)]
    
    // Pasos de importación del archivo CSV de vendedores
    #"CSV importado" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 4, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado", [PromoteAllScalars = true]),
    
    // 2. LIMPIEZA Y TRANSFORMACIÓN DE LA TABLA BASE
    #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {{"seller_id", type text}, {"seller_zip_code_prefix", Int64.Type}, {"seller_city", type text}, {"seller_state", type text}}, "es"),
    
    // Estandarizar ciudades a formato de título
    #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Tipo de columna cambiado", {{"seller_city", each Text.Proper(_), type nullable text}}),
    
    // Asegurar que el código postal sea texto (clave de unión)
    #"Tipo de columna cambiado 1" = Table.TransformColumnTypes(#"Mayúsculas aplicadas en cada palabra", {{"seller_zip_code_prefix", type text}}),
    
    // Quitar duplicados en códigos postales de vendedores (si un vendedor aparece múltiples veces)
    #"Duplicados quitados" = Table.Distinct(#"Tipo de columna cambiado 1", {"seller_zip_code_prefix"}),

    // 3. UNIÓN CON GEOLOCALIZACIÓN
    // Une los vendedores (por ZIP/código postal) con la tabla de Geolocalización
    #"Consultas combinadas Geolocation" = Table.NestedJoin(#"Duplicados quitados", {"seller_zip_code_prefix"}, #"olist_geolocation_dataset csv", {"geolocation_zip_code_prefix"}, "olist_geolocation_dataset csv", JoinKind.LeftOuter),
    
    // Expande Latitud y Longitud
    #"olist_geolocation_dataset csv expandido" = Table.ExpandTableColumn(#"Consultas combinadas Geolocation", "olist_geolocation_dataset csv", {"geolocation_lat", "geolocation_lng"}, {"geolocation_lat", "geolocation_lng"}),
    
    // Renombra Lat/Lng a nombres claros
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"olist_geolocation_dataset csv expandido", {{"geolocation_lat", "latitud"}, {"geolocation_lng", "longitud"}}),
    
    // 4. MANEJO DE VALORES NULOS (Establece 0 si no hay coordenadas)
    #"Valor reemplazado" = Table.ReplaceValue(#"Columnas con nombre cambiado", null, 0, Replacer.ReplaceValue, {"longitud", "latitud"}),
    
    // Recorte de espacios en el estado (limpieza final)
    #"Texto recortado" = Table.TransformColumns(#"Valor reemplazado", {{"seller_state", each Text.Trim(_), type nullable text}}),
    
    // 5. UNIÓN CON LA TABLA DE ESTADOS (DimState)
    // Une por la sigla del estado para obtener el nombre completo (estado_completo)
    #"Consultas combinadas DimState" = Table.NestedJoin(#"Texto recortado", {"seller_state"}, DimState, {"sigla"}, "DimState", JoinKind.LeftOuter),
    #"DimState expandido" = Table.ExpandTableColumn(#"Consultas combinadas DimState", "DimState", {"estado_completo"}, {"estado_completo"}),
    
    // 6. LIMPIEZA FINAL DE COLUMNAS
    #"Columnas reordenadas" = Table.ReorderColumns(#"DimState expandido", {"seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "estado_completo", "latitud", "longitud"}),
    
    // Quita la columna de sigla original del estado (seller_state)
    #"Columnas quitadas" = Table.RemoveColumns(#"Columnas reordenadas", {"seller_state"}),
    
    // Renombra el estado completo (estado_completo) a seller_state
    #"Columnas con nombre cambiado 1" = Table.RenameColumns(#"Columnas quitadas", {{"estado_completo", "seller_state"}})
in
    #"Columnas con nombre cambiado 1"
