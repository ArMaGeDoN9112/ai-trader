# Смена моделей через OpenRouter

Проверено 2026-11-06 по официальным материалам OpenRouter. Интеграция не реализована.

**Вывод:** выбор отдельный для привязки публикаций к компаниям, анализа компании с торговым предложением и критика. Текстовые модели меняются через `model`; Jev требует другого протокола.

## Два протокола

Для текстовых/reasoning-моделей: `POST https://openrouter.ai/api/v1/chat/completions`, OpenAI-совместимый клиент с base URL `https://openrouter.ai/api/v1` и ключом OpenRouter. [OpenAI SDK](https://openrouter.ai/docs/guides/community/openai-sdk), [Chat Completions](https://openrouter.ai/docs/guides/features/structured-outputs).

Jev доступен через `POST https://openrouter.ai/api/v1/systemone`: `model`, `state`, `questions` → `answers`. TypeSafe SDK получает base URL `https://openrouter.ai/api` и сам добавляет `/v1/systemone`; ключ — OpenRouter. Подтверждены преобразования `jev-1.13` → `typesafe/jev-1.13`, `jev-latest` → `~typesafe/jev-latest`; ID с префиксом используются как есть. Ответ содержит фактические `model`, `provider`, `id`, `usage.cost`. [TypeSafe SDK](https://openrouter.ai/docs/guides/community/typesafe-sdk).

Та же страница подтверждает `TYPESAFE_BASE_URL` и `TYPESAFE_API_KEY`, но не `TYPESAFE_DEFAULT_MODEL`. Поддержка значения `jev-latest` в поле запроса подтверждена; чтение этой переменной конкретной версией SDK здесь не проверено. Для проекта передавать выбранный `model` явно.

Актуальная модель в руководстве — `typesafe/jev-1.14`; также описан отдельный `POST /api/alpha/decisions`. Jev отвечает Noul/Choice/Score, а не генерирует объяснения. Поэтому замена Jev на текстовую модель требует адаптации входа, разбора и проверки выхода. Не смешивать этот контракт с `chat/completions`. [Jev](https://openrouter.ai/docs/guides/community/jev).

## Строгий выход и ошибки

Для текстовых ролей передавать `response_format.type: "json_schema"`, схему и `strict: true`, плюс `provider.require_parameters: true`. Поддержка `structured_outputs` зависит от endpoint провайдера и меняется со временем. Даже strict имеет различия между провайдерами; приложение обязано проверять JSON, схему и смысловые ограничения. Невалидный или оборванный ответ не становится торговым предложением. [Structured Outputs](https://openrouter.ai/docs/guides/features/structured-outputs).

Без `require_parameters` неподдерживаемые параметры могут игнорироваться: предпочтение поддерживающих endpoint не спасает, когда их нет вообще. При обязательной схеме нужно завершать вызов ошибкой, без повторения запроса с ослабленной схемой. [Provider routing](https://openrouter.ai/docs/guides/routing/provider-selection).

## Фиксация выбора

Межмодельный fallback включается списком `models`; фактическая модель возвращается в `model`. Для требования «выбранная модель или ошибка» использовать один явный ID, без такого списка и автоматического router/плавающего alias. [Model fallbacks](https://openrouter.ai/docs/guides/routing/model-fallbacks).

Fallback между провайдерами одной модели — отдельная настройка, по умолчанию разрешён. `provider.order` задаёт порядок, `allow_fallbacks: false` ограничивает им переходы; один точный endpoint в списке фиксирует провайдера. Если допускаются несколько провайдеров, каждый должен поддерживать обязательные параметры. [Provider routing](https://openrouter.ai/docs/guides/routing/provider-selection).

Настройки reasoning зависят от модели; reasoning-токены оплачиваются и обычно расходуют общий лимит выхода. [Reasoning](https://openrouter.ai/docs/guides/best-practices/reasoning-tokens).

## Решения для проекта

Наши инженерные решения:

- Хранить по роли протокол, model ID и параметры; сохранять проверяемую схему при смене модели.
- При старте цикла фиксировать версию конфигурации целиком. Аналитик, критик и повторная проверка внутри него используют этот снимок; сохранённое изменение применяется со следующего цикла.
- Записывать запрошенный и фактический model, provider, ID вызова, версию конфигурации/схемы, расход и ошибку. Снимок не гарантирует идентичного повторного результата.

Для Chat Completions можно получить сведения о выборе provider через `X-OpenRouter-Metadata: enabled`: `openrouter_metadata` содержит выбранный endpoint и попытки. Метаданные отсутствуют при cache hit и некоторых ошибках; отсутствие нельзя подменять предположением о провайдере. System One возвращает provider непосредственно. [Router metadata](https://openrouter.ai/docs/guides/features/router-metadata), [System One](https://openrouter.ai/docs/api/api-reference/systemone/submit-a-system-one-request).

Начальные текстовые модели и бюджет не выбраны. Платные вызовы, аккаунтный доступ, качество на русских новостях и задержки не проверялись; это остаётся риском следующего эксперимента.
