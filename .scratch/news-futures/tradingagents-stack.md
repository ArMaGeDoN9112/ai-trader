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

## Применение у нас

Рекомендация: LangGraph StateGraph + LangChain Core + Pydantic для трёх ролей; для аналитика/критика через OpenRouter — актуальный официальный **langchain-openrouter / ChatOpenRouter**. Наш выбор отличается от upstream. Structured output зависит от модели: function_calling по умолчанию либо json_schema. Jev System One — отдельный HTTP-контракт узла. [Официальная интеграция](https://docs.langchain.com/oss/python/integrations/chat/openrouter).

Telegram MTProto — адаптер публикаций; Т-Банк MCP sandbox — адаптер отдельного программного исполнителя. Доменные публикации/привязки, причина входа и позиция хранятся отдельно; checkpoint нужен при durable recovery, не заменяет идемпотентность заявки/сверку брокера. Лимиты риска обязательны согласно [ADR 0001](../../docs/adr/0001-separate-proposals-from-execution.md).

Не копировать 12 ролей/CLI. Free-text без схемы не допускается к исполнению. LangSmith — opt-in. [Fallback][structured].

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

