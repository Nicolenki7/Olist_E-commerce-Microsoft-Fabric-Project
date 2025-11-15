// ETL_DimProduct.m
// Propósito: Creación de la dimensión 'DimProduct'. Combina los datos de los productos con la tabla de traducción
// de categorías para obtener nombres legibles en inglés y realiza la limpieza de texto.
//
// Fuentes de datos:
// - olist_products_dataset csv (Tabla base del producto)
// - product_category_name_translation csv (Traducción de categorías)

let
    // 1. CARGA E INICIO DE LA TABLA BASE
    Origen_Product = Lakehouse.Contents(null),
    // [Se omiten pasos de navegación específicos de Fabric (Lakehouse.Contents, Navegación, etc.)]

    // Pasos de importación del archivo CSV de productos
    #"CSV importado Productos" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 9, QuoteStyle = QuoteStyle.None]),
    #"Encabezados promovidos" = Table.PromoteHeaders(#"CSV importado Productos", [PromoteAllScalars = true]),
    
    // 2. CONVERSIONES DE TIPO
    #"Tipo de columna cambiado" = Table.TransformColumnTypes(#"Encabezados promovidos", {
        {"product_id", type text}, 
        {"product_category_name", type text}, 
        {"product_name_lenght", type number}, 
        {"product_description_lenght", type number}, 
        {"product_photos_qty", type number}, 
        {"product_weight_g", type number}, 
        {"product_length_cm", type number}, 
        {"product_height_cm", type number}, 
        {"product_width_cm", type number}
    }, "es"),
    
    // 3. LIMPIEZA DE CATEGORÍA
    // Estandariza el nombre de la categoría a Mayúsculas en cada Palabra (Text.Proper)
    #"Mayúsculas aplicadas en cada palabra" = Table.TransformColumns(#"Tipo de columna cambiado", {{"product_category_name", each Text.Proper(_), type nullable text}}),
    
    // 4. UNIÓN CON LA TRADUCCIÓN
    // Une la tabla de productos con la tabla de traducción por el nombre de la categoría en portugués
    #"Consultas combinadas Translation" = Table.NestedJoin(#"Mayúsculas aplicadas en cada palabra", {"product_category_name"}, #"product_category_name_translation csv", {"product_category_name"}, "product_category_name_translation csv", JoinKind.LeftOuter),
    
    // Expande el nombre traducido (product_category_name_english)
    #"product_category_name_translation csv expandido" = Table.ExpandTableColumn(#"Consultas combinadas Translation", "product_category_name_translation csv", {"product_category_name_english"}, {"product_category_name_english"}),
    
    // 5. LIMPIEZA FINAL DE COLUMNAS
    
    // Reordenar columnas (paso opcional, pero mantiene el orden de tu Dataflow)
    #"Columnas reordenadas" = Table.ReorderColumns(#"product_category_name_translation csv expandido", {
        "product_id", "product_category_name", "product_category_name_english", "product_name_lenght", 
        "product_description_lenght", "product_photos_qty", "product_weight_g", 
        "product_length_cm", "product_height_cm", "product_width_cm"
    }),
    
    // Quita la columna de categoría original en portugués
    #"Columnas quitadas" = Table.RemoveColumns(#"Columnas reordenadas", {"product_category_name"}),
    
    // Renombra la columna de categoría en inglés para que sea el nombre final del campo dimensional
    #"Columnas con nombre cambiado" = Table.RenameColumns(#"Columnas quitadas", {{"product_category_name_english", "product_category_name"}})

in
    #"Columnas con nombre cambiado"
