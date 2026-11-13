# News-futures: краткое руководство

Приложение отслеживает новости Telegram и сохраняет решения по фьючерсам. Путь редакции: категория и компании → событие и проверка позиции → выбор серии → анализ и проверка критиком. Связь новости с компанией сама по себе не даёт сигнала на вход.

## Запуск

Из корня проекта: Python 3.11, окружение `.venv`, зависимости из `requirements-telegram.txt`, `requirements-openrouter.txt` и `requirements-sandbox.txt`. Скопируй [шаблоны](../config/) и [шаблон ключей](../credentials.example.toml) в `.local` без суффикса `.example`. Заполни ключи Telegram, OpenRouter и песочницы; сессию Telegram храни вне репозитория.

~~~powershell
.venv\Scripts\python -m news_futures telegram-login
.venv\Scripts\python -m news_futures sandbox-diagnostics .local/catalog --config .local/sandbox-settings.json
~~~

Укажи `account_id` из отчёта диагностики в `.local/runtime.json`. Запусти `.venv\Scripts\python -m news_futures serve .local/runtime.json`. Для панели сначала выполни `npm.cmd --prefix news_futures/operator/web ci` и `npm.cmd --prefix news_futures/operator/web run build`, затем замени `serve` на `operator-ui` и нажми **Запустить**.

## Настройки

- [Профиль](../config/runtime.example.json): режим, счёт, каталог, `data_dir`, интервалы и лимиты агента. Пути считаются от файла профиля; его правки требуют перезапуска.
- [Decisions](../news_futures/agents/decisions-model.json): один `model` для категорий, компаний, выбора фьючерса и следующего чтения в `tool_agent`. По умолчанию `parallel/decisions-fast:free`; при ошибке запасная модель не вызывается. Для своего профиля скопируй файл в `.local` и укажи его в поле `decisions`. Панель отображает эту модель, а изменения в панели относятся только к чат-ролям.
- Чат-роли: [OpenRouter](../news_futures/agents/openrouter-models.json) или [Groq](../news_futures/agents/groq-models.json); отдельная модель координатора задаётся в `coordinator`. Модели фиксируются на редакцию, изменения применяются к следующей.
- Остальные справочники: [каналы Telegram](../news_futures/adapters/telegram-sources.json), [категории фьючерсов](../news_futures/adapters/tbank/futures-categories.json), [лимиты риска](../news_futures/trading/risk-policy.json).

## Путь публикации и полномочия

`fixed` передаёт аналитику готовый снимок. В `tool_agent` координатор делегирует разбор, а агент выбирает чтение архива или рынка в пределах назначенной компании и контракта. Критик проверяет точную версию предложения; допускается одна доработка. Одобрение критика — предложение, а не исполненная заявка.

`observation` читает брокера. Запись в песочнице доступна только отдельному исполнителю после независимых проверок риска. Автономный вход заблокирован, пока не подтверждена защита от двойного исполнения SL/TP. При неизвестном исходе исполнитель сверяет журнал и брокера перед любым повторным действием. Тестовые новости из панели не допускаются к исполнению.

## Диагностика и проверка

- Начни с `runtime-status .local/runtime.json --decision ID` или трассы в панели: проверь статус, причину остановки и запрошенную и фактическую модели. Трассы и контрольные точки лежат в `data_dir`; для восстановления используй тот же каталог.
- Профиль и очередь — в [application/](../news_futures/application/), выбор модели и анализ — в [agents/](../news_futures/agents/), риск и заявки — в [trading/](../news_futures/trading/). Аргументы отдельных команд смотри через `python -m news_futures <команда> --help`.
- Офлайн-проверки: `python -m unittest discover -s tests -v` и `python -m mypy --strict news_futures tests` из окружения проекта. При правке Decisions проверь ID во всех четырёх шагах и отказ без подмены.

История изменений — в [тикетах](../.scratch/news-futures/issues/) и [ADR](adr/).
