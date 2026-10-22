## Indicadores Clave de Rendimiento (KPIs) y Reglas de Alerta

> **Variante 5:** Demanda por zonas  
> **Riesgo Principal:** Saturación y Déficit de Flota
* **Problema:** Picos masivos de solicitudes por zona que superan la capacidad de preparación en cocinas y la disponibilidad de repartidores, generando pedidos en estado `CREADO` sin asignar, incumplimiento de SLA y cancelaciones.
---

### 1. Definición de KPIs Mínimos

| KPI | Métrica Técnica | Ventana de Cálculo | Unidad | Objetivo Operacional |
| :--- | :--- | :--- | :--- | :--- |
| **KPI 1: Tasa de Demanda por Zona** | `kpi_pedidos_por_minuto` | Deslizante de 5 min (paso 1 min) | `pedidos/min` | Monitorear el ritmo de entrada de órdenes en tiempo real para anticipar congestión en comercios y escasez de repartidores. |
| **KPI 2: Concentración de Zona Caliente** | `kpi_share_demanda_zona` | Fija (*tumbling*) de 15 min | `% del total` | Detectar cuadrantes que absorben una porción desproporcionada de la demanda metropolitana para redistribuir la flota. |
| **KPI 3: Demanda por Franja y Categoría** | `kpi_demanda_franja_categoria` | Fija de 1 hora | `conteo` | Segmentar el volumen de órdenes por horario (pico almuerzo, pico cena, valle) y tipo de comercio (Restaurante, Supermercado, Farmacia). |

#### Fórmulas de Cálculo (Procesamiento en Spark)
* **KPI 1 (Rendimiento):**
  $$\text{Throughput}(z, t) = \frac{\text{Conteo de pedidos en zona } z \text{ en la ventana } [t - 5\text{ min}, t]}{5\text{ minutos}}$$
  
* **KPI 2 (% Participación):**
  $$\ \text{ Demanda}(z) = \left( \frac{\text{Pedidos en Zona } z}{\sum_{k \in \text{Zonas}} \text{Pedidos en Zona } k} \right) \times 100$$
  
* **KPI 3 (Segmentación horaria):**
  $$\text{Volumen}(f, c) = \text{COUNT}(\text{pedidos}) \quad \forall \text{ timestamp} \in \text{Franja } f \land \text{categoría} = c$$

---

### 2. Reglas de Alerta Operacionales y Umbrales Iniciales

| Alerta | Evento Disparador | Umbral Inicial | Nivel | Acción Operativa Automatizada |
| :--- | :--- | :--- | :--- | :--- |
| **ALERTA_DEMANDA_SUPERIOR_UMBRAL** | Picos de solicitudes por zona | **Warning:** $> 35$ ped/min<br>**Critical:** $> 50$ ped/min | `CRITICAL` | 1. Activar tarifa dinámica (*surge pricing*) para regular la entrada de órdenes.<br>2. Notificar a repartidores en zonas aledañas con incentivo por viaje. |
| **ALERTA_ZONA_SIN_CAPACIDAD** | Acumulación de órdenes en espera | $> 80$ pedidos en estado `CREADO` sin asignar por $> 10$ min | `CRITICAL` | 1. Pausa preventiva de admisión de nuevos pedidos en la app para esa zona (*pause orders*).<br>2. Rebalanceo forzado de la flota. |

#### Justificación Técnica y de Negocio de los Umbrales

* **Justificación Alerta 1 (Sobredemanda > 50 ped/min):**  
  La capacidad promedio estimada de preparación de los comercios aliados en un radio de cobertura zonal es de 25 a 30 pedidos por minuto. Superar los 35 pedidos/minuto satura los tiempos de preparación en cocinas, y superar los 50 pedidos/minuto agota los repartidores disponibles en el cuadrante, elevando exponencialmente el tiempo de entrega.
  
* **Justificación Alerta 2 (Capacidad Insuficiente > 80 pedidos sin asignar por > 10 min):**  
  Tener más de 80 pedidos sin chofer asignado durante más de 10 minutos garantiza que el tiempo total de entrega superará los 45 minutos (violando el SLA prometido de RapidGo). Esta alerta protege la reputación del servicio evitando cancelaciones masivas.

---
