"""Construye docs/report.html de Olist a partir del resumen mensual agregado.

Los CSV de pedidos e ítems del dataset público de Olist no se versionan por tamaño;
se agregan una vez y se versiona el resumen que consume el informe.

Uso: python src/build_report.py
"""
import pathlib
import sys

import pandas as pd

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from report import escribir, num

ROOT = pathlib.Path(__file__).resolve().parents[1]
m = pd.read_csv(ROOT / "data" / "olist_resumen_mensual.csv")
est = pd.read_csv(ROOT / "data" / "olist_estado_pedidos.csv")
m["anio"] = m.mes.str[:4]

filas = []
for _, r in m.iterrows():
    filas.append({"a": str(r.anio), "g": "gtv", "k": r.mes, "v": round(float(r.gtv / 1000), 1), "u": "k"})
    filas.append({"a": str(r.anio), "g": "pedidos", "k": r.mes, "v": int(r.pedidos), "u": ""})
    filas.append({"a": str(r.anio), "g": "flete", "k": r.mes, "v": float(r.flete_pct), "u": "%"})
for _, r in est.iterrows():
    filas.append({"a": "todos", "g": "estado", "k": str(r.estado), "v": round(100 * r.pedidos / est.pedidos.sum(), 1), "u": "%"})

por_anio = m.groupby("anio").agg(gtv=("gtv", "sum"), pedidos=("pedidos", "sum"), flete=("flete_pct", "mean"))
tabla = [[a, num(len(m[m.anio == a])), num(int(r.pedidos)), f"{num(r.gtv / 1000, 0)} k R$",
          f"{num(r.gtv / r.pedidos, 2)} R$", f"{num(r.flete, 1)} %"] for a, r in por_anio.iterrows()]

escribir(
    ROOT / "docs" / "report.html",
    eyebrow="E-commerce · Olist (muestra pública)",
    titulo="Olist: cuánto vende y cuánto cuesta el envío",
    pregunta=("El dataset público de Olist tiene pedidos brasileños de 2016 a 2018. La pregunta que "
              "de verdad mueve decisiones en un marketplace no es el total facturado, sino cuánto de "
              "ese total se lo come el flete y en qué meses el volumen se cae, porque ahí está el "
              "margen del vendedor."),
    kpis=[("Ventana", f"{m.mes.min()} → {m.mes.max()}", "24 meses con datos"),
          ("Pedidos", num(int(m.pedidos.sum())), "estados pagados o enviados"),
          ("GTV", f"{num(m.gtv.sum() / 1000000, 2)} M R$", "suma de price de los ítems"),
          ("Flete sobre GTV", f"{num(100 * m.flete.sum() / m.gtv.sum(), 1)} %", "peso del envío")],
    filtros=["todos"] + sorted(m.anio.unique()),
    filtro_etiqueta="Año",
    graficos=[
        {"id": "g", "grupo": "gtv", "titulo": "GTV por mes", "subtitulo": "miles de reales facturados", "unidad": "k"},
        {"id": "p", "grupo": "pedidos", "titulo": "Pedidos por mes", "subtitulo": "pedidos distintos con fecha de compra", "unidad": ""},
        {"id": "f", "grupo": "flete", "titulo": "Flete sobre GTV", "subtitulo": "qué porcentaje del valor se va en envío", "unidad": "%"},
        {"id": "e", "grupo": "estado", "titulo": "Estado de los pedidos", "subtitulo": "% del total de pedidos del período", "unidad": "%"},
    ],
    filas=filas,
    tabla={"titulo": "El año, en una fila", "subtitulo": "2016 y 2018 están incompletos: el dataset corta en septiembre",
           "cabeceras": ["Año", "Meses", "Pedidos", "GTV", "Ticket medio", "Flete medio"], "filas": tabla},
    metodo=["Fuente: dataset público de Olist (olist_order_items_dataset y olist_orders_dataset), agregado una vez y versionado.",
            "Solo se cuentan pedidos con estado pagado, aprobado, facturado, enviado o entregado; cancelados quedan fuera del GTV.",
            "GTV = suma de price de los ítems; el flete se informa aparte, no se suma a la facturación.",
            "El ticket medio es GTV / pedidos distintos, no el ticket de una línea."],
    limitaciones=["2016 y 2018 están incompletos: el dataset corta a principios de septiembre de 2018.",
                  "No hay coste del producto, así que no se puede calcular margen, solo ingreso bruto.",
                  "Los precios están en reales de 2016-2018, sin ajuste por inflación.",
                  "No se versionan los CSV originales (32 MB); el resumen es lo que queda auditable en el repo."],
    fuente="data/olist_resumen_mensual.csv y data/olist_estado_pedidos.csv",
    metodo_sub="Agregado mensual calculado una vez desde el dataset público y versionado.",
    lim_sub="Lo que este recorte no permite afirmar.")
print("docs/report.html:", (ROOT / "docs" / "report.html").stat().st_size, "bytes")
