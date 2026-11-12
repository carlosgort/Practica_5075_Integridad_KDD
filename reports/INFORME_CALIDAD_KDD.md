# Informe de Perfilado de Datos y Calidad KDD (ISO/IEC 25012)

- **Fecha de generación:** 2026-11-12 20:09:54
- **Dataset listo para BI/KDD:** `processed/dataset_kdd_listo.csv`
- **Esquema validado:** `id_transaccion, tienda, timestamp, categoria, importe_eur, unidades, metodo_pago`

## 1. Indicadores de Calidad de Datos (ISO/IEC 25012)

| Dimensión / Indicador | Fórmula / Cálculo | Resultado |
| :--- | :--- | :---: |
| **Tasa de Integridad Criptográfica (Fase 1)** | `2 validados / 3 totales × 100` | **66.67%** |
| **Tasa de Completitud en Origen (Lote Bruto)** | `95 sin nulos / 100 brutos × 100` | **95.00%** |
| **Tasa de Completitud Final (Dataset KDD)** | `87 sin nulos / 87 finales × 100` | **100.00%** |
| **Tasa de Validez Neta del Lote** | `87 válidos / 100 brutos × 100` | **87.00%** |
| **Tasa de Unicidad (`id_transaccion`)** | `87 únicos / 87 finales × 100` | **100.00%** |

## 2. Distribución Estadística de Variables Clave (Post-Limpieza)

| Métrica Estadística | `importe_eur` (€) | `unidades` (uds.) |
| :--- | :---: | :---: |
| Conteo ($n$) | 87 | 87 |
| **Media** | **70.99 €** | **3.79** |
| **Mediana (P50)** | **73.44 €** | **4.00** |
| **Desviación Estándar ($s$)** | **26.34 €** | **1.69** |
| **Percentil 25 (Q1)** | **53.80 €** | **2.00** |
| **Percentil 75 (Q3)** | **85.34 €** | **5.00** |
| Rango Intercuartílico (IQR) | 31.54 € | 3.00 |
| Mínimo | 12.50 € | 1 |
| Máximo | 124.47 € | 6 |
| Volumen Total Agregado | 6175.83 € | 330 |

## 3. Conclusiones Técnicas y Justificación KDD

1. **Protección frente a manipulación (Integridad):** El rechazo en Fase 1 de `transacciones_tienda_sur.csv` evita que registros alterados (`ALTERADO_NO_AUTORIZADO` e importes inyectados de `9999.99 €`) distorsionen los KPIs de negocio.
2. **Criterio de descarte vs. imputación conservadora (Completitud y Exactitud):** Los registros sin `id_transaccion` o sin `importe_eur` se enviaron a cuarentena porque inventar claves primarias rompe la trazabilidad e imputar importes sesga la varianza. En cambio, los 2 nulos en variables categóricas secundarias (`categoria` y `metodo_pago`) se imputaron con la moda para conservar el 100% de ingresos reales válidos.
3. **Coherencia temporal y de dominio (Consistencia):** La validación ISO 8601 y el filtrado de fechas fuera del ejercicio 2026 (`2024`, `2027` y `2026-99-99`), así como importes `<= 0` y unidades `< 1`, garantiza series temporales y márgenes consistentes en BI.
4. **Control de outliers mediante IQR (Estabilidad analítica):** Aislar los 3 registros extremos (`1.480,00 €`, `2.750,90 €` y `4.120,00 €`) reduce la media de `importe_eur` de `161,41 €` (`s = 528,78 €`) a `70,99 €` (`s = 26,34 €`), alineándola con la mediana (`73,44 €`) y evitando desplazamientos de centroides en algoritmos KDD (clustering, regresión o detección de fraude).

## 4. Jerarquía de Directorios Generada tras la Ejecución

```text
practica_kdd/
├── quarantine/
│   ├── tampered/
│   │   └── transacciones_tienda_sur.csv (3894 B)
│   ├── bad_records.csv (1608 B)
│   └── outliers.csv (586 B)
├── staging/
│   ├── cleaned/
│   │   └── transacciones_limpias.csv (7069 B)
│   └── verified/
│       ├── transacciones_tienda_este.csv (3975 B)
│       └── transacciones_tienda_norte.csv (4046 B)
└── processed/
    ├── data_profiling_summary.json (3888 B)
    ├── data_profiling_summary.md (2928 B)
    └── dataset_kdd_listo.csv (7069 B)
```
