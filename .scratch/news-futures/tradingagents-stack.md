# TradingAgents: стек по исходникам

2026-11-06: повторная проверка GitHub API — `main = 9fa1196fffbcfd9de5146d2dd2869df84ab99838` (2026-11-04). Прочитаны 94 Python-файла приложения/CLI, манифест, CI и выбранные тесты; код закреплён. [Ветка][branch].

## Фактический стек

| Слой | Фактическое применение |
|---|---|
| Оркестрация | `langgraph>=1.2.12`: StateGraph/MessagesState, ToolNode, условные edges. LangChain Core ≥1.6.5: prompts/messages/tools, `prompt \| llm.bind_tools(...)`. [Граф][setup], [ход аналитика][turn]. |
| LLM | langchain-openai/-anthropic/-google-genai; своя ленивая фабрика quick/deep tiers. **Upstream OpenRouter — ChatOpenAI из langchain-openai**, URL шлюза в реестре. [Фабрика][factory], [OpenAI-compatible][openai]. |
| Схемы | Pydantic BaseModel/Field/validators, with_structured_output → Markdown; при сбое возможен free-text fallback. Pydantic импортируется, отдельно в dependencies не указан. [Схемы][schemas], [fallback][structured]. |
| Checkpoints | langgraph-checkpoint-sqlite + stdlib sqlite3: SqliteSaver, БД на тикер, thread id с подписью настроек. По умолчанию выключены; compile без saver. [Checkpointer][checkpoint], [lifecycle][graph]. |
| Данные | yfinance/pandas/stockstats — цены/таблицы/индикаторы; requests — HTTP/Jev; Reddit — urllib + XML Atom/RSS, StockTwits — urllib; pytz — FRED. dateutil импортируется, отдельно не объявлен. [Market][market], [vendors][router], [RSS][reddit]. |
| CLI/прочее | Typer/Rich/Questionary — CLI; packaging — обновления; markdown-it-py — HTML; dotenv — env; cryptography — токен ChatGPT sign-in. Всё объявлено runtime, хотя часть нужна только выбранному сценарию. [Манифест][manifest], [CLI][cli], [auth][auth]. |
| Optional/dev | Extras: dev = Ruff/pytest/pytest-subtests; bedrock = langchain-aws. CI: Python 3.11–3.14, timezone-run, clean-install smoke, Ruff; отдельного type-check job нет. [Манифест][manifest], [CI][ci]. |

Python ≥3.11, setuptools, entry point cli.main:app; requirements.txt содержит только `.`. Версии ограничены снизу; lock-файла, заброшенных объявленных пакетов и метапакета langchain нет. В приложении/CLI нет LangSmith, Chroma/vector-store, AutoGen/CrewAI, MemorySaver/InMemorySaver; это не исключает транзитивные пакеты/tracing через env. [Манифест][manifest], [requirements][requirements], [дерево][tree].

## Карта модулей

- **graph/trading_graph.py** — модели, запуск, checkpoint lifecycle, запись решений. **setup.py/analyst_execution.py** — параллельные выбранные аналитики с private message subgraphs, отдельными report keys и ограниченными tool rounds. Одновременно Memory Log; barrier ждёт всех. [Координатор][graph], [сборка][setup].
- Далее Bull ↔ Bear → Research Manager → Trader → три risk-debaters → Portfolio Manager → END; conditional_logic ограничивает обсуждения. При четырёх аналитиках — 12 LLM-ролей; Memory Log выполняет settlement/reflection, не является тринадцатым аналитиком. [Граф][setup].
- **agents/** — prompts, schemas, узлы; **tools.py** — read-only инструменты с тикером/датой из InjectedState; **dataflows/router.py** — Yahoo, Alpha Vantage, SEC EDGAR, FRED, Polymarket. **llm_clients/** — wrappers/capabilities/providers. [Tools][tools], [роутер][router], [фабрика][factory].
- **memory/log.py, settlement.py, reflection.py** — Markdown pending/outcome/lessons, последние записи своего/других тикеров с историческим cutoff. Это **не embeddings/RAG и не checkpoint state**. [Журнал][memory], [координатор][graph].

**Jev используется:** Sentiment Analyst → StockTwits/Reddit screen → requests POST /v1/systemone, без TypeSafe SDK. Проверяется отношение поста к **заданному тикеру** и stance автора; это не готовая маршрутизация публикации по компаниям и не проверка истинности. Без ключа/при сбое посты остаются непроверенными. [Вызов][sentiment], [HTTP/fallback][jev].

## Как устроено приложение

2026-11-08 GitHub Compare подтвердил, что `main` совпадает с ревизией `9fa1196fffbcfd9de5146d2dd2869df84ab99838`. Выводы ниже относятся к исходникам TradingAgents 0.7.1. [Сравнение][compare], [версия][version].

- **Запуск.** После установки пакета команда `tradingagents` запускает анализ тикера на заданную дату. В терминале недостающие параметры запрашиваются, для запуска без терминала они задаются флагами и переменными окружения. Из Python тот же граф вызывается через `TradingAgentsGraph.propagate(...)`, который возвращает состояние и итоговую оценку. CLI использует потоковые обновления графа для отображения прогресса. Оба пути используют общий код записи решения и экспорта отчёта. Docker запускает ту же команду и сохраняет данные на подключённом томе. [Манифест][manifest], [CLI][cli], [подготовка запуска][cli-run], [вызов из Python][python-usage], [Docker][docker], [том][compose].
- **Настройки и адаптеры.** Переменные `TRADINGAGENTS_*` дополняют значения по умолчанию. Python может передать конфигурацию явно, а CLI собирает её из ввода и окружения. Адаптеры данных читают настройки текущего запуска. Роутер выбирает поставщика по типу данных с переопределением для отдельной функции получения данных. Явно заданный список определяет порядок запасных поставщиков. Ответы различают недоступность поставщика и отсутствие данных по инструменту. [Настройки][defaults], [настройки запуска][cli-run], [контекст конфигурации][run-config], [выбор поставщика][routing].
- **Граф.** Граф связывает выбранные read-only аналитические ветки с общей стадией разбора аргументов и решения. Каждый аналитик возвращает свой отчёт, а отведённое ему число вызовов инструментов ограничено. Обсуждение начинается после сбора отчётов. Итоговый узел Portfolio Manager формирует оценку, затем граф завершается. [Сборка][setup], [инструменты][tools].
- **Результаты и восстановление.** Запись завершённого решения сохраняет JSON состояния и запись в Markdown-журнале решений. Отдельный экспорт собирает Markdown по этапам, сводный Markdown и, по умолчанию, автономный HTML. Настройки отчёта перечисляют версию, выбранные модели, аналитиков и поставщиков. Ключи, URL и локальные пути исключены из этого списка. Опциональная SQLite-точка восстановления имеет свой жизненный цикл и очищается после успешного завершения. JSON привязан к тикеру и дате, поэтому повторный запуск той же пары заменяет этот файл. [Запись решения и настройки][graph-output], [JSON][state-output], [генератор отчёта][reporting], [контрольные точки][checkpoint].
- **Граница исследования.** В README описана отправка заявки на симулированную биржу, но в закреплённой реализации анализ заканчивается решением. Backtest запускает тот же граф по сетке тикеров и дат, сохраняет отдельный журнал, пропускает обработанные пары при повторном запуске с тем же `run_id` и оценивает доходность относительно ориентира по ценам закрытия после даты анализа. Ячейки независимы, а переданный портфель остаётся одним снимком. Оценка качества решений не моделирует заявки, исполнения и движение денег. Текстовые ленты не архивируются, поэтому исторический результат имеет ограниченную воспроизводимость. [Описание][research-readme], [завершение графа][setup], [границы и реализация backtest][backtest], [оценка по закрытиям][settlement].

Для нашего приложения из этого следует единая устанавливаемая точка запуска, которая собирает одни и те же настройки, хранилище и адаптеры для ручной диагностики и длительной работы. Отдельные контракты публикаций, рыночных данных и моделей позволяют менять поставщиков и воспроизводить сценарии. Из журнала цикла можно получить читаемый отчёт с публикациями, привязками, торговым предложением, проверками лимитов, статусом заявки и подтверждением защиты. Историческое воспроизведение использует доступные на тот момент входы и отдельное состояние. Отправка и сверка заявок в песочнице остаются обязанностью программного исполнителя. [Спецификация](spec.md), [ADR 0001](../../docs/adr/0001-separate-proposals-from-execution.md), [ADR 0002](../../docs/adr/0002-langgraph-langchain-agent-workflows.md).

## Применение у нас

Стек для трёх ролей — LangGraph StateGraph, LangChain Core и Pydantic. Политика стека предусматривает `ChatOpenRouter`, а текущая реализация аналитика и критика использует совместимый `ChatOpenAI`, чтобы сохранить сведения о провайдере. Для торговых предложений требуется строгая JSON Schema. Jev System One — отдельный HTTP-контракт узла. [Политика стека](../../docs/agents/agent-stack.md), [проверка совместимости](../../docs/agents/openrouter-compatibility.md).

Telegram MTProto — адаптер публикаций; Т-Банк MCP sandbox — адаптер отдельного программного исполнителя. Доменные публикации/привязки, причина входа и позиция хранятся отдельно; checkpoint нужен при durable recovery, не заменяет идемпотентность заявки/сверку брокера. Лимиты риска обязательны согласно [ADR 0001](../../docs/adr/0001-separate-proposals-from-execution.md).

Сохранить три согласованные роли и спроектировать команды вокруг сценариев нашего приложения. К проверкам исполнения допускаются предложения с валидированной схемой. LangSmith включается по выбору пользователя. [Fallback][structured].

Тесты offline: scripted full graph, параллельность, crash/resume; fixture запрещает сеть вне integration. [Тесты][tests], [fixtures][fixtures]. Не запускались upstream/платные API; не измерены качество Jev на косвенных связях, задержки, стоимость и наши Telegram/MCP-интеграции. Прибыльность не проверялась.

[branch]: https://api.github.com/repos/TauricResearch/TradingAgents/branches/main
[tree]: https://api.github.com/repos/TauricResearch/TradingAgents/git/trees/9fa1196fffbcfd9de5146d2dd2869df84ab99838?recursive=1
[setup]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/graph/setup.py#L52-L194
[turn]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/analysts/turn.py#L14-L28
[factory]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/llm_clients/factory.py
[openai]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/llm_clients/openai_client.py#L239-L287
[schemas]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/schemas.py
[structured]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/structured.py#L43-L108
[checkpoint]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/graph/checkpointer.py
[graph]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/graph/trading_graph.py
[market]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/dataflows/vendors/yahoo/market.py
[router]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/dataflows/router.py#L9-L149
[reddit]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/dataflows/vendors/reddit.py
[manifest]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/pyproject.toml
[cli]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/cli/main.py
[auth]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/llm_clients/chatgpt_auth.py#L113-L143
[ci]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/.github/workflows/ci.yml
[requirements]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/requirements.txt
[tools]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/tools.py
[memory]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/memory/log.py#L1-L133
[sentiment]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/analysts/sentiment_analyst.py#L47-L76
[jev]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/agents/post_screen.py
[tests]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tests/test_graph_end_to_end.py
[fixtures]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tests/conftest.py#L59-L75
[compare]: https://github.com/TauricResearch/TradingAgents/compare/9fa1196fffbcfd9de5146d2dd2869df84ab99838...main
[version]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/__init__.py#L1-L14
[cli-run]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/cli/run.py
[python-usage]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/README.md#L244-L266
[docker]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/Dockerfile#L26-L30
[compose]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/docker-compose.yml#L1-L9
[defaults]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/default_config.py
[run-config]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/dataflows/config.py#L44-L72
[routing]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/dataflows/router.py#L160-L285
[graph-output]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/graph/trading_graph.py#L247-L405
[state-output]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/graph/trading_graph.py#L438-L476
[reporting]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/reporting.py#L20-L136
[research-readme]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/README.md#L66-L106
[backtest]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/backtest.py
[settlement]: https://github.com/TauricResearch/TradingAgents/blob/9fa1196fffbcfd9de5146d2dd2869df84ab99838/tradingagents/memory/settlement.py#L44-L110

