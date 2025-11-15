// ETL_FactOrders.m
// Propósito: Creación de la tabla de hechos 'FactOrders'. Esta consulta consolida todas las transacciones 
// (órdenes, ítems, pagos, reseñas) y prepara las columnas para el análisis DAX.
//
// NOTA: Las referencias '#"olist_orders_dataset csv"' y otras representan las conexiones
// a las tablas de origen en tu Dataflow de Fabric.

let
    // 1. CARGA DE LA TABLA PRINCIPAL (Items, desde raw_csv_olist)
    Origen_Items = Lakehouse.Contents(null),
    // [Se omiten pasos de navegación específicos de Fabric (workspaceId, lakehouseId)]
    // El paso de importación inicial comienza con la tabla de ítems, que es el centro de la Fact
    #"CSV importado Items" = Csv.Document(#"Navegación 4", [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
    #"Encabezados promovidos Items" = Table.PromoteHeaders(#"CSV importado Items", [PromoteAllScalars = true]),
    
    // 2. CONVERSIONES DE TIPO INICIALES EN ITEMS
    TablaItemsBase = Table.TransformColumnTypes(#"Encabezados promovidos Items", {
        {"shipping_limit_date", type datetime}, 
        {"order_id", type text}, 
        {"order_item_id", type text}, 
        {"product_id", type text}, 
        {"seller_id", type text}, 
        {"price", type number}, 
        {"freight_value", type number}
    }),
  
    // 3. UNIÓN CON ÓRDENES
    // Une Items (base) con las Órdenes para obtener customer_id y fechas
    #"Consultas combinadas Orders" = Table.NestedJoin(TablaItemsBase, {"order_id"}, #"olist_orders_dataset csv", {"order_id"}, "olist_orders_dataset csv", JoinKind.Inner),
    #"olist_orders_dataset csv expandido" = Table.ExpandTableColumn(#"Consultas combinadas Orders", "olist_orders_dataset csv", 
        {"customer_id", "order_purchase_timestamp", "order_delivered_customer_date"}, 
        {"customer_id", "order_purchase_timestamp", "order_delivered_customer_date"}),

    // 4. UNIÓN CON PAGOS
    // Une con Pagos (asume Inner Join, aunque Left Outer es a menudo preferido en Fact)
    #"Consultas combinadas Payments" = Table.NestedJoin(#"olist_orders_dataset csv expandido", {"order_id"}, #"olist_order_payments_dataset csv", {"order_id"}, "olist_order_payments_dataset csv", JoinKind.Inner),
    #"olist_order_payments_dataset csv expandido" = Table.ExpandTableColumn(#"Consultas combinadas Payments", "olist_order_payments_dataset csv", 
        {"payment_type", "payment_value"}, 
        {"payment_type", "payment_value"}),

    // 5. UNIÓN CON RESEÑAS
    // Une con Reseñas (Left Outer Join)
    #"Consultas combinadas Reviews" = Table.NestedJoin(#"olist_order_payments_dataset csv expandido", {"order_id"}, #"olist_order_reviews_dataset csv", {"order_id"}, "olist_order_reviews_dataset csv", JoinKind.LeftOuter),
    #"olist_order_reviews_dataset csv expandido" = Table.ExpandTableColumn(#"Consultas combinadas Reviews", "olist_order_reviews_dataset csv", 
        {"review_score"}, 
        {"review_score"}),

    // 6. UNIÓN CON FECHA ESTIMADA DE ENTREGA (del segundo join a la tabla Orders)
    // El Dataflow realiza un segundo join a la tabla Orders para obtener order_estimated_delivery_date
    #"Consultas combinadas Estimated Date" = Table.NestedJoin(#"olist_order_reviews_dataset csv expandido", {"order_id"}, #"olist_orders_dataset csv", {"order_id"}, "olist_orders_dataset csv", JoinKind.Inner),
    #"olist_orders_dataset csv expandido 1" = Table.ExpandTableColumn(#"Consultas combinadas Estimated Date", "olist_orders_dataset csv", 
        {"order_estimated_delivery_date"}, 
        {"order_estimated_delivery_date"}),

    // 7. TRANSFORMACIONES FINALES

    // Reordenar Columnas para un modelo final limpio
    #"Columnas reordenadas final" = Table.ReorderColumns(#"olist_orders_dataset csv expandido 1", {
        "order_item_id", "order_id", "product_id", "seller_id", "customer_id", "review_score", 
        "shipping_limit_date", "order_purchase_timestamp", "order_delivered_customer_date", 
        "order_estimated_delivery_date", "price", "freight_value", "payment_type", "payment_value"
    }),

    // Asegurar el tipo de columna final para review_score
    #"Tipo de columna cambiado final" = Table.TransformColumnTypes(#"Columnas reordenadas final", {{"review_score", Int64.Type}}),
    
    // Reemplazo de Nulos por una fecha base (1900-01-01) en las columnas de fecha
    // Esta es una técnica para manejar fechas nulas en algunos sistemas, aunque se recomienda DAX para esto.
    #"Valor reemplazado" = Table.ReplaceValue(#"Tipo de columna cambiado final", null, #datetime(1900, 1, 1, 0, 0, 0), Replacer.ReplaceValue, 
        {"shipping_limit_date", "order_purchase_timestamp", "order_delivered_customer_date", "order_estimated_delivery_date"})

in
    #"Valor reemplazado"
