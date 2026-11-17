# News-futures

React-панель находится в [frontend/](frontend/), Python API и обработчик — в [backend/](backend/).
После настройки `.local` запусти `docker compose up --build -d`. Compose поднимет PostgreSQL и сохранит данные в именованном томе `postgres`.
При обновлении существующей установки сначала [перенеси данные из SQLite](docs/runtime.md#переход-с-sqlite).
Панель открывается по адресу [127.0.0.1:8765](http://127.0.0.1:8765/).

[Краткое руководство по проекту](docs/runtime.md).
