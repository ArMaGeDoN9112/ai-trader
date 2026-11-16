# News-futures: краткое руководство

Приложение отслеживает новости Telegram и сохраняет решения по фьючерсам. Путь редакции: категория и компании → событие и проверка позиции → выбор серии → анализ и проверка критиком. Связь новости с компанией сама по себе не даёт сигнала на вход.

## Запуск

Понадобится Docker с Compose. Из корня проекта скопируй [шаблоны](../config/) и [шаблон ключей](../credentials.example.toml) в `.local` без суффикса `.example`, если этих файлов ещё нет. Заполни ключи Telegram, OpenRouter или Groq и песочницы согласно профилю. Один раз авторизуй Telegram в контейнере. Если каталога брокера ещё нет, выполни диагностику:

~~~powershell
docker compose build backend
docker compose run --rm backend telegram-login
docker compose run --rm backend sandbox-diagnostics .local/catalog --config .local/sandbox-settings.json
~~~

Укажи `account_id` из отчёта диагностики в `.local/runtime.json`. Если переходишь с локального запуска Python, сначала останови прежний обработчик. Выполни `docker compose up --build -d`, открой [127.0.0.1:8765](http://127.0.0.1:8765/) и нажми **Запустить**. Контейнер бэкенда запускает API; обработчик остаётся остановленным до команды из панели.

Фронтенд обслуживает Nginx, он же передаёт запросы `/api/` во внутреннюю сеть Compose. Бэкенд видит хостовую папку `.local` как `/app/backend/.local`: храни в ней профиль, дополнительные файлы настроек, сертификаты и рабочий архив. Используй относительные пути к этим файлам в профиле. Оставь поле `session` незаданным: Compose хранит сессию Telegram в томе `telegram`. `docker compose down` сохраняет том; флаг `-v` удаляет сессию.

Для остановки стека используй `docker compose down`, для журналов — `docker compose logs -f backend frontend`. После правок файлов рабочего профиля перезапусти бэкенд командой `docker compose restart backend` и обнови вкладку.

## Настройки

- [Профиль](../config/runtime.example.json): режим, счёт, каталог, `data_dir`, интервалы и лимиты агента. Пути считаются от файла профиля; его правки требуют перезапуска.
- [Decisions](../backend/news_futures/application/analysis/decisions-model.json): один `model` для категорий, компаний, выбора фьючерса и следующего чтения в `tool_agent`. По умолчанию `parallel/decisions-fast:free`; при ошибке запасная модель не вызывается. Для своего профиля скопируй файл в `.local` и укажи его в поле `decisions`. Панель отображает эту модель, а изменения в панели относятся только к чат-ролям.
- Чат-роли: [OpenRouter](../backend/news_futures/application/analysis/openrouter-models.json) или [Groq](../backend/news_futures/application/analysis/groq-models.json); отдельная модель координатора задаётся в `coordinator`. Модели фиксируются на редакцию, изменения применяются к следующей.
- Остальные справочники: [каналы Telegram](../backend/news_futures/adapters/telegram-sources.json), [категории фьючерсов](../backend/news_futures/adapters/tbank/futures-categories.json), [лимиты риска](../backend/news_futures/domain/risk-policy.json).

## Путь публикации и полномочия

`fixed` передаёт аналитику готовый снимок. В `tool_agent` координатор делегирует разбор, а агент выбирает чтение архива или рынка в пределах назначенной компании и контракта. Критик проверяет точную версию предложения; допускается одна доработка. Одобрение критика — предложение, а не исполненная заявка.

`observation` читает брокера. Запись в песочнице доступна только отдельному исполнителю после независимых проверок риска. Автономный вход заблокирован, пока не подтверждена защита от двойного исполнения SL/TP. При неизвестном исходе исполнитель сверяет журнал и брокера перед любым повторным действием. Тестовые новости из панели не допускаются к исполнению.

## Архитектура бэкенда

- [domain](../backend/news_futures/domain/) содержит модель публикации, контракт анализа, правила расчёта объёма и сверки исполнения.
- [application](../backend/news_futures/application/) ведёт очередь редакций, анализ и торговые сценарии. Интерфейсы брокера, моделей и хранилищ описаны в [ports](../backend/news_futures/application/ports/). Графы LangGraph управляют шагами анализа, а решения и трассы хранятся в локальных JSON-артефактах.
- [adapters](../backend/news_futures/adapters/) содержит CLI, HTTP, SQLite и интеграции с Telegram, моделями и T‑Bank. [bootstrap](../backend/news_futures/bootstrap/) подключает реализации к сценариям. При сохранении резерва и подтверждении закрытия SQLite удерживает блокировку ревизии до фиксации торгового намерения.

Контрольные точки LangGraph используют имена Python-классов. Перед обновлением завершите текущий анализ и остановите воркер. Архивы публикаций и событий, журнал сделок и готовые отчёты используют прежние форматы.

## Диагностика и проверка

- Начни с `docker compose exec backend python -m news_futures runtime-status .local/runtime.json --decision ID` или трассы в панели: проверь статус, причину остановки и запрошенную и фактическую модели. Трассы и контрольные точки лежат в `data_dir`; для восстановления используй тот же каталог.
- Аргументы команд доступны через `docker compose run --rm backend <команда> --help`.
- Образ бэкенда устанавливает [requirements.txt](../backend/requirements.txt). Файл [constraints.txt](../backend/constraints.txt) фиксирует версии для Docker и локальной установки.
- Для офлайн-проверок выполни `python -m pip install -r requirements-dev.txt` из `backend/` в окружении Python 3.11. Этот список включает зависимости приложения. Затем выполни `python -m unittest discover -s tests -v` и `python -m mypy --strict news_futures tests`. Для проверки в Chrome собери фронтенд (`npm ci` и `npm run build` из `frontend/`) и перед запуском тестов укажи путь к браузеру в `OPERATOR_TEST_CHROME`. При правке Decisions проверь ID во всех четырёх шагах и отказ без подмены.

История изменений — в [тикетах](../.scratch/news-futures/issues/) и [ADR](adr/).
