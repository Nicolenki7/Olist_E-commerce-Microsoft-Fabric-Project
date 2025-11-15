// ETL_DimCustomer.m
// Propósito: Creación de la dimensión 'DimCustomer'. Limpia los datos del cliente, realiza una traducción manual 
// de las siglas de estado a nombres completos y añade coordenadas geográficas.
//
// Fuentes de datos:
// - olist_customers_dataset csv (Tabla base del cliente)
// - Geoloc (Tabla de geolocalización, asumiendo que ha sido pre-procesada y cargada)

let
    // 1. CARGA E INICIO DE LA TABLA BASE
    Origen_Customer = Lakehouse.Contents(null),
    // [Se omiten pasos de navegación específicos de Fabric (Lakehouse.Contents, Navegación, etc.)]

    // Pasos de importación del archivo CSV de clientes
    #"CSV importado Clientes" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 5, QuoteStyle = QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado Clientes", [PromoteAllScalars = true]),
    
    // 2. CONVERSIONES DE TIPO Y RENOMBRADO INICIAL
    #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {
        {"customer_id", type text}, 
        {"customer_unique_id", type text}, 
        {"customer_zip_code_prefix", Int64.Type}, 
        {"customer_city", type text}, 
        {"customer_state", type text}
    }, "es"),
    
    // Renombrado explícito (paso opcional pero mantenido de tu JSON)
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Tipo de columna cambiado", {
        {"customer_id", "customer_id"}, 
        {"customer_unique_id", "customer_unique_id"}, 
        {"customer_zip_code_prefix", "customer_zip_code_prefix"}, 
        {"customer_city", "customer_city"}, 
        {"customer_state", "customer_state"}}),
    
    // 3. TRADUCCIÓN MANUAL DE SIGLAS DE ESTADO (Limpieza de datos manual)
    // Se realizan múltiples reemplazos para traducir las siglas (SP, SC, MG, etc.) a nombres de estado
    // completos para la columna 'customer_state'.
    #"Valor reemplazado SP" = Table.ReplaceValue(#"Columnas con nombre cambiado", "SP", "Sao Paulo", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado SC" = Table.ReplaceValue(#"Valor reemplazado SP", "SC", "Santa Catarina", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado MG" = Table.ReplaceValue(#"Valor reemplazado SC", "MG", "Minas Gerais", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado RJ" = Table.ReplaceValue(#"Valor reemplazado MG", "RJ", "Rio de Janeiro", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado RS" = Table.ReplaceValue(#"Valor reemplazado RJ", "RS", "Rio Grande do Sul", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado PA" = Table.ReplaceValue(#"Valor reemplazado RS", "PA", "Pará", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado GO" = Table.ReplaceValue(#"Valor reemplazado PA", "GO", "Goiás", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado MA" = Table.ReplaceValue(#"Valor reemplazado GO", "MA", "Maranhao", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado PR" = Table.ReplaceValue(#"Valor reemplazado MA", "PR", "Paraná", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado BA" = Table.ReplaceValue(#"Valor reemplazado PR", "BA", "Bahía", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado ES" = Table.ReplaceValue(#"Valor reemplazado BA", "ES", "Espirito Santo", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado MS" = Table.ReplaceValue(#"Valor reemplazado ES", "MS", "Mato Grosso do Sul", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado CE" = Table.ReplaceValue(#"Valor reemplazado MS", "CE", "Ceará", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado DF" = Table.ReplaceValue(#"Valor reemplazado CE", "DF", "Distrito Federal", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado RN" = Table.ReplaceValue(#"Valor reemplazado DF", "RN", "Rio Grande do Norte", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado AL" = Table.ReplaceValue(#"Valor reemplazado RN", "AL", "Alagoas", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado PE" = Table.ReplaceValue(#"Valor reemplazado AL", "PE", "Pernambuco", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado AM" = Table.ReplaceValue(#"Valor reemplazado PE", "AM", "Amazonas", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado MT" = Table.ReplaceValue(#"Valor reemplazado AM", "MT", "Mato Grosso", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado TO" = Table.ReplaceValue(#"Valor reemplazado MT", "TO", "Tocantins", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado RO" = Table.ReplaceValue(#"Valor reemplazado TO", "RO", "Rondonia", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado SE" = Table.ReplaceValue(#"Valor reemplazado RO", "SE", "Sergipe", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado AP" = Table.ReplaceValue(#"Valor reemplazado SE", "AP", "Amapá", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado PB" = Table.ReplaceValue(#"Valor reemplazado AP", "PB", "Paraíba", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado PI" = Table.ReplaceValue(#"Valor reemplazado PB", "PI", "Piauí", Replacer.ReplaceText, {"customer_state"}),
    #"Valor reemplazado AC" = Table.ReplaceValue(#"Valor reemplazado PI", "AC", "Acre", Replacer.ReplaceText, {"customer_state"}),
    
    // 4. LIMPIEZA DE TEXTO Y FORMATO FINAL
    
    // Estandarizar ciudades a formato de título
    #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Valor reemplazado AC", {{"customer_city", each Text.Proper(_), type nullable text}}),
    
    // Asegurar que el código postal sea texto (clave de unión)
    #"Tipo de columna cambiado 1" = Table.TransformColumnTypes(#"Mayúsculas aplicadas en cada palabra", {{"customer_zip_code_prefix", type text}}),
    
    // Pasos de limpieza de texto adicionales (Trim, Clean)
    #"Texto recortado final" = Table.TransformColumns(#"Tipo de columna cambiado 1", {{"customer_state", each Text.Trim(_), type nullable text}}),
    #"Texto limpio" = Table.TransformColumns(#"Texto recortado final", {{"customer_state", each Text.Clean(_), type nullable text}}),
    
    // Reemplazar posibles cadenas vacías resultantes de la limpieza por nulo
    #"Valor reemplazado null" = Table.ReplaceValue(#"Texto limpio", "", null, Replacer.ReplaceValue, {"customer_state"}),
    
    // 5. UNIÓN CON GEOLOCALIZACIÓN
    // Une los clientes (por ZIP/código postal) con la tabla de Geolocalización (Geoloc)
    #"Consultas combinadas Geolocation" = Table.NestedJoin(#"Valor reemplazado null", {"customer_zip_code_prefix"}, Geoloc, {"geolocation_zip_code_prefix"}, "Geoloc", JoinKind.LeftOuter),
    
    // Expande Latitud y Longitud
    #"Geoloc expandido" = Table.ExpandTableColumn(#"Consultas combinadas Geolocation", "Geoloc", {"geolocation_lat", "geolocation_lng"}, {"geolocation_lat", "geolocation_lng"}),
    
    // Renombra Lat/Lng a nombres claros
    #"Columnas con nombre cambiado 1" = Table.RenameColumns(#"Geoloc expandido", {{"geolocation_lat", "latitud"}, {"geolocation_lng", "longitud"}}),
    
    // 6. MANEJO DE VALORES NULOS (Establece 0 si no hay coordenadas)
    #"Valor reemplazado latitud" = Table.ReplaceValue(#"Columnas con nombre cambiado 1", null, 0, Replacer.ReplaceValue, {"latitud"}),
    #"Valor reemplazado longitud" = Table.ReplaceValue(#"Valor reemplazado latitud", null, 0, Replacer.ReplaceValue, {"longitud"})  

in
    #"Valor reemplazado longitud";

// Consulta auxiliar (Tabla de Geolocalización)
shared Geoloc = let
  Origen = Lakehouse.Contents(null),
  // [Se omiten pasos de navegación específicos de Fabric]
  #"CSV importado" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 5, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado", [PromoteAllScalars = true]),
  #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {{"geolocation_zip_code_prefix", type text}, {"geolocation_lat", Int64.Type}, {"geolocation_lng", Int64.Type}, {"geolocation_city", type text}, {"geolocation_state", type text}}, "es"),
  // Limpieza y estandarización del texto en la tabla Geoloc
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Tipo de columna cambiado", {
    {"geolocation_zip_code_prefix", each Text.Proper(Text.From(_)), type nullable text}, 
    {"geolocation_city", each Text.Proper(Text.From(_)), type nullable text}}),
  #"Texto recortado" = Table.TransformColumns(#"Mayúsculas aplicadas en cada palabra", {
      {"geolocation_zip_code_prefix", each Text.Trim(_), type nullable text}, 
      {"geolocation_city", each Text.Trim(_), type nullable text}, 
      {"geolocation_state", each Text.Trim(_), type nullable text}}),
  #"Texto en mayúsculas" = Table.TransformColumns(#"Texto recortado", {{"geolocation_state", each Text.Upper(_), type nullable text}}),
  // Eliminar duplicados por código postal para usarlo como clave 
  #"Duplicados quitados" = Table.Distinct(#"Texto en mayúsculas", {"geolocation_zip_code_prefix"}),
  #"Tipo de columna cambiado 1" = Table.TransformColumnTypes(#"Duplicados quitados", {{"geolocation_lat", type number}, {"geolocation_lng", type number}}),
  #"Duplicados quitados 1" = Table.Distinct(#"Tipo de columna cambiado 1", {"geolocation_zip_code_prefix"})
in
  #"Duplicados quitados 1"
