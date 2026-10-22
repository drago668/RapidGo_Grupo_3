# RapidGo Grupo 3

## caso de estudio

RapidGo es una plataforma de entregas que necesita una plataforma de datos capaz de capturar eventos operacionales, procesarlos, almacenarlos, generar indicadores y automatizar procesos de control. Todos los equipos trabajarán sobre la misma organización, pero resolverán problemas diferentes.

--- 

### *Variante 5* **demanda por zonas**

> **Eventos:** zona, timestamp, categoria, estado
> **KPIs:** pedidos/minuto, zona caliente, franja
> **Alertas:** demanda alta / capacidad insuficiente

 

### Estructura proyecto

```Shell
bash
├── README.md
├── data
│   ├── curated
│   ├── processed
│   └── raw
├── datalake
│   └── README.md
├── docker-compose.yml
├── docs
│   ├── checkpoints
│   └── decisions
├── producer
├── requirements.txt
├── spark
└── tests
```
