// ETL_ProductCategoryTranslation.m
// Propósito: Carga y limpieza de la tabla de traducción de categorías.
// Utilizado en DimProduct para mapear Portugués a Inglés.

shared #"product_category_name_translation csv" = let
  Origen = Lakehouse.Contents(null),
  // Asumiendo que #"Navegación 4" apunta a "product_category_name_translation.csv"
  
  #"CSV importado" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 2, Encoding = 65001, QuoteStyle = QuoteStyle.None]),
  #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado", [PromoteAllScalars = true]),
  
  // Limpieza de formato (Mayúsculas en cada palabra)
  #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Encabezados promovidos", {{"product_category_name", each Text.Proper(_), type nullable text}}),
  #"Mayúsculas aplicadas en cada palabra 1" = Table.TransformColumns(#"Mayúsculas aplicadas en cada palabra", {{"product_category_name_english", each Text.Proper(_), type nullable text}})
in
  #"Mayúsculas aplicadas en cada palabra 1"
