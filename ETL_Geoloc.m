// ETL_Geoloc.m
// Propósito: Carga y limpieza de la tabla de Geolocalización.
// Utilizado para obtener Latitud y Longitud en DimCustomer y DimSeller.

shared Geoloc = let
  Origen = Lakehouse.Contents(null),
  // Asumiendo que #"Navegación 4" apunta a "olist_geolocation_dataset.csv"
  
  #"CSV importado" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 5, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado", [PromoteAllScalars = true]),
  
  // Limpieza y Estandarización
  #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {
    {"geolocation_zip_code_prefix", type text}, {"geolocation_lat", Int64.Type}, 
    {"geolocation_lng", Int64.Type}, {"geolocation_city", type text}, {"geolocation_state", type text}
  }, "es"),
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Tipo de columna cambiado", {
    {"geolocation_zip_code_prefix", each Text.Proper(Text.From(_)), type nullable text}, 
    {"geolocation_city", each Text.Proper(Text.From(_)), type nullable text}}),
    
  // Limpieza final de texto y eliminación de duplicados
  #"Texto en mayúsculas" = Table.TransformColumns(#"Mayúsculas aplicadas en cada palabra", {{"geolocation_state", each Text.Upper(_), type nullable text}}),
  #"Duplicados quitados" = Table.Distinct(#"Texto en mayúsculas", {"geolocation_zip_code_prefix"}),
  
  // Conversión final de tipos
  #"Tipo de columna cambiado 1" = Table.TransformColumnTypes(#"Duplicados quitados", {{"geolocation_lat", type number}, {"geolocation_lng", type number}})
in
  #"Tipo de columna cambiado 1"
