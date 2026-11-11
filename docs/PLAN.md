# Plan de ejecucion

## Estado actual
- 7 scripts de Power Query (`.m`) que construyen el modelo dimensional, `.pbix` e informe.
- `.gitattributes` evita que GitHub los detecte como Wolfram Language.

## Proximos pasos
1. Documentar el grafo de dependencias entre los `.m` (que tabla depende de cual) en `docs/lineage.md`.
2. Convertir los `.m` a notebooks ejecutables o a `.pq` con convencion de nombres.
3. Mover `.pbix`, `.pdf` y `.docx` a `docs/` y dejar la raiz limpia.
4. `docs/model.md` con el diagrama del star schema y las claves de cada tabla.

## Criterio de aceptacion
- Cualquier persona puede reconstruir el modelo siguiendo `docs/lineage.md`.

## Riesgos
- Power Query no es testeable de forma nativa: el contrato se valida en el destino.
