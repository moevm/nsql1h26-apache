# Запуск проекта

Требуется установленный `git`, `docker` и `docker compose`.

## 1. Клонирование репозитория

```bash
git clone https://github.com/moevm/nsql1h26-apache.git
cd nsql1h26-apache
```

## 2. Сборка и запуск

Проект поднимает три контейнера:

- `nsql-apache-app`
- `nsql-apache-db`
- `nsql-apache-mongo-express`

Для запуска выполнить:

```bash
docker compose build --no-cache
docker compose up -d
```

## 3. Открыть приложение

После запуска доступны:

- приложение: [http://localhost:8000](http://localhost:8000)
- Swagger: [http://localhost:8000/docs](http://localhost:8000/docs)
- Mongo Express: [http://localhost:8081](http://localhost:8081)

Для `mongo-express`:

- логин: `admin`
- пароль: `pass`

Основной сценарий просмотра приложения начинается с адреса:

- [http://localhost:8000](http://localhost:8000)

## 4. Генерация тестовых логов

Для генерации входных файлов выполнить:

```bash
docker compose exec -T app python scripts/generate_logs.py --output-dir /app/generated_logs --access-count 300 --error-count 80 --days 10 --seed 42
```

Команда создаст файлы:

- `/app/generated_logs/access.log`
- `/app/generated_logs/error.log`
- `/app/generated_logs/mixed.log`

Если нужно скопировать их из контейнера на хост:

```bash
docker cp nsql-apache-app:/app/generated_logs/. ./generated_logs
```

После этого на хосте будут доступны:

- `./generated_logs/access.log`
- `./generated_logs/error.log`
- `./generated_logs/mixed.log`

## 5. Остановка контейнеров

Остановить контейнеры:

```bash
docker compose down
```

Остановить контейнеры и удалить данные базы:

```bash
docker compose down -v
```
