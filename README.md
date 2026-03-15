# Kafka Lab - Платформа для каршеринга

Лабораторная работа №1 "ПОТОКОВАЯ ОБРАБОТКА ДАННЫХ В РЕАЛЬНОМ ВРЕМЕНИ С ИСПОЛЬЗОВАНИЕМ БРОКЕРА APACHE KAFKA"

## Описание

Реализован механизм обработки потоковых данных для системы каршеринга с использованием Kafka. Producer имитирует бортовой компьютер автомобиля, который генерирует события о начале, ходе и завершении поездки (ID автомобиля, ID пользователя, текущие координаты, выбранный тариф, пробег и уровень топлива/заряда).

Consumer перехватывает эти события, сопоставляет их с разрешенными зонами парковки и рассчитывает финальную стоимость на основе тарифа (минуты + километраж).

Для демонстрации системы обработки ошибок около 30% событий генерируются как «критические»: уровень топлива/заряда, завершение поездки вне зоны.

## Стек технологий

- **Apache Kafka 4.2.0** (KRaft mode, без ZooKeeper) — брокер сообщений
- **Python 3** — язык разработки скриптов
- **kafka-python** — клиентская библиотека для работы с Kafka из Python

## Структура проекта

```
kafka-lab/
├── generator.py      # Генерация случайных сообщений (вне Producer — принцип SOLID)
├── producer.py       # Отправка сообщений в Kafka-топик
├── consumer.py       # Чтение и валидация сообщений из Kafka-топика
└── requirements.txt  # Зависимости Python
```

## Формат сообщения

```json
{
  "cars": "Toyota Camry",
  "users": "Ivanov Ivan",
  "zones": "Zone A",
  "tarrifs": "per_minute",
  "conditions": "excellent"
}
```

## Правила валидации (Consumer)

| Поле | Условие невалидности |
|------|----------------------|
| `fare` | `<= 0` |
| `distance_km` | `< 0` |
| `duration_min` | `<= 0` |

## Запуск

### Требования
- Java 17+
- Python 3.8+
- Apache Kafka 4.2.0

### 1. Установить зависимости Python

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Инициализировать Kafka (только один раз)

```cmd
cd C:\path\to\kafka_2.13-4.2.0

bin\windows\kafka-storage.bat random-uuid
bin\windows\kafka-storage.bat format --standalone -t <UUID> -c config\server.properties
```

### 3. Запустить Kafka (окно 1)

```cmd
bin\windows\kafka-server-start.bat config\server.properties
```

### 4. Запустить Consumer (окно 2)

```cmd
venv\Scripts\activate
python consumer.py
```

### 5. Запустить Producer (окно 3)

```cmd
venv\Scripts\activate
python producer.py
```

## Пример вывода

**Producer:**
```
SEND: {"car": "VW Polo", "user": "Ivanov Ivan", "pickup_zone": "Zone C", "dropoff_zone": "Zone D", "tariff": "per_minute", "duration_min": 84, "distance_km": 7.8, "fare": -429.31, "car_condition": "good"}
SEND: {"car": "Kia Rio", "user": "Sidorov Dmitry", "pickup_zone": "Zone C", "dropoff_zone": "Zone B", "tariff": "per_km", "duration_min": 113, "distance_km": 49.1, "fare": 1227.5, "car_condition": "good"}
```

**Consumer:**
```
RECEIVED: {"car": "Kia Rio", "user": "Sidorov Dmitry", "pickup_zone": "Zone C", "dropoff_zone": "Zone B", "tariff": "per_km", "duration_min": 113, "distance_km": 49.1, "fare": 1227.5, "car_condition": "good"}
NOT VALID: {"car": "VW Polo", "user": "Ivanov Ivan", "pickup_zone": "Zone C", "dropoff_zone": "Zone D", "tariff": "per_minute", "duration_min": 84, "distance_km": 7.8, "fare": -429.31, "car_condition": "good"}
```
