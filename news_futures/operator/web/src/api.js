import { useEffect, useState } from 'react';

const session = document.querySelector('meta[name="operator-session"]').content;

export async function request(path, body, signal = AbortSignal.timeout(8000)) {
  const headers = { 'X-Operator-Session': session };
  const options = { headers, signal, cache: 'no-store' };
  if (body !== undefined) {
    options.method = 'POST';
    headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(body);
    if (path === '/api/settings') options.signal = AbortSignal.timeout(60000);
  }
  const response = await fetch(path, options);
  const value = await response.json();
  if (!response.ok) throw new Error(value.result);
  return value;
}

export function useResource(path, refresh, interval = 0) {
  const [state, setState] = useState({});
  useEffect(() => {
    if (!path) return;
    const controller = new AbortController();
    let timer;
    async function load() {
      try {
        const signal = AbortSignal.any([controller.signal, AbortSignal.timeout(8000)]);
        const data = await request(path, undefined, signal);
        if (!controller.signal.aborted) setState({ path, data });
      } catch (error) {
        if (!controller.signal.aborted) {
          setState(previous => ({ path, data: previous.path === path ? previous.data : undefined, error: error.message }));
        }
      } finally {
        if (interval && !controller.signal.aborted) timer = setTimeout(load, interval);
      }
    }
    load();
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [path, refresh, interval]);
  return state.path === path ? state : {};
}

export const route = (screen, ...parts) => '#' + [screen, ...parts.filter(Boolean)].map(encodeURIComponent).join('/');
export const query = (path, params) => path + '?' + new URLSearchParams(params);
export const emptyNews = { companies: {}, contracts: {}, decisions: [], publications: [], orders: [] };
export const companyName = (news, id) => news.companies[id]?.names[0] || id;
export const contractName = (news, id) => news.contracts[id]?.ticker || id;
export const contractOptions = (news, company) => Object.entries(news.contracts)
  .filter(([, item]) => !company || item.company_id === company)
  .map(([id]) => [id, contractName(news, id)]).sort((a, b) => a[1].localeCompare(b[1], 'ru'));

const dateFormat = new Intl.DateTimeFormat('ru-RU', { dateStyle: 'short', timeStyle: 'medium' });
export const time = value => value && Number.isFinite(Date.parse(value)) ? dateFormat.format(new Date(value)) : '—';

const labels = {
  stopped: 'Остановлен', starting: 'Запускается', running: 'Работает', stopping: 'Останавливается',
  failed: 'Ошибка', connected: 'Подключён', unknown: 'Неизвестно', unavailable: 'Недоступен',
  observation: 'Наблюдение', sandbox: 'Песочница', none: 'Нет', confirmed: 'Подтверждена',
  fixed: 'Фиксированный маршрут', tool_agent: 'Агент с инструментами',
  tool_agent_requires_observation: 'Агент с инструментами доступен в режиме наблюдения.',
  invalid_analysis_strategy: 'Выберите допустимый маршрут анализа.',
  candidate: 'Кандидат на вход', insufficient_data: 'Недостаточно данных', cached: 'Из истории запуска',
  not_reviewed: 'Без заключения', agent_budget: 'Исчерпан бюджет анализа',
  agent_deadline: 'Истекло время анализа', specialist_budget: 'Исчерпан лимит вызовов роли',
  unknown_tool: 'Инструмент недоступен', invalid_tool_call: 'Вызов инструмента не прошёл проверку',
  unknown_evidence: 'Наблюдение не найдено в этом запуске', unknown_proposal_reference: 'Предложение не найдено',
  stale_market_context: 'Данные рынка устарели', market_not_available: 'Торговая сессия недоступна',
  broker_read_timeout: 'Брокер не ответил в пределах времени на проверку рынка',
  event_decision_superseded: 'Решение по событию обновилось', publication_revision_superseded: 'Вышла новая редакция публикации',
  unconfirmed: 'Не подтверждена', long: 'Лонг', short: 'Шорт', protected: 'Защита подтверждена',
  processing: 'Не завершено', saved: 'Сохранено', completed: 'Завершено', continue: 'Пройдено',
  skipped: 'Пропущено', rejected: 'Отклонено', blocked: 'Заблокировано', approved: 'Одобрено',
  pending: 'Ожидает обработки', no_text: 'Без текста', context_only: 'Только контекст',
  no_allowed_company: 'Нет подходящей компании',
  position_already_open: 'По этому тикеру уже открыта позиция',
  account_position_limit: 'На счёте уже открыта позиция по другому тикеру',
  no_category_candidates: 'Нет компаний для проверки по выбранным категориям',
  category_catalog_unavailable: 'Каталог категорий фьючерсов недоступен',
  no_action: 'Без торгового действия', approved_for_checks: 'Одобрено для проверки',
  repeat: 'Повтор', update: 'Обновление', uncertain_update: 'Спорное обновление',
  uncertain_match: 'Возможное совпадение', unmatched: 'Отдельное событие', model_error: 'Ошибка модели',
  event_model_error: 'Ошибка модели сопоставления события', runtime_profile_changed: 'Рабочий профиль изменился',
  OpenAITimeoutError: 'Сервис модели не ответил в пределах тайм-аута',
  APITimeoutError: 'Сервис модели не ответил в пределах тайм-аута',
  OpenAIRateLimitError: 'Сервис модели ограничил частоту запросов',
  invalid_model_response: 'Ответ модели не прошёл проверку формата',
  invalid_candidate_assessment: 'Ответ модели противоречит правилам сопоставления',
  unverifiable_source_quote: 'Цитата не совпадает с текстом указанной редакции',
  unverifiable_independence_evidence: 'Цитата для независимого подтверждения не найдена в публикации',
  unexpected_independence_evidence: 'Модель вернула цитату без вывода о независимом подтверждении',
  http_429: 'Сервис ограничил частоту запросов (HTTP 429)',
  http_502: 'Ошибка шлюза сервиса (HTTP 502)', http_503: 'Сервис временно недоступен (HTTP 503)',
  http_504: 'Сервис не ответил вовремя (HTTP 504)', service_unavailable: 'Не удалось связаться с сервисом',
  invalid_decision: 'Ответ модели не прошёл проверку', model_mismatch: 'Сервис вернул другую модель',
  CredentialsError: 'Не удалось прочитать данные доступа',
  publication_before_start: 'Публикация вышла до запуска обработчика',
  skip: 'Предложено пропустить', hold: 'Предложено удерживать', enter: 'Вход', close: 'Выход',
  closed: 'Закрыт', open: 'Открыт', reserved: 'Зарезервирован', no_fill: 'Без исполнения',
  intent: 'Намерение записано', response_received: 'Ответ брокера получен', response_rejected: 'Брокер отказал',
  response_uncertain: 'Ответ брокера не подтверждён', reconciliation: 'Сверка', confirmed_fills: 'Подтверждённые исполнения',
  reviewed_close: 'Новостной выход допущен', reviewed_close_abandoned: 'Новостной выход отменён',
  manual_close: 'Подтверждение ручного выхода', manual_close_rejected: 'Ручное закрытие отклонено',
  close_queued: 'Запрос принят. Обработчик сверит позицию и защиту.',
  queued: 'Ожидает сверки', accepted: 'Подтверждено. Ожидается результат брокера.',
  manual_exit: 'Ручное закрытие', news_exit: 'Новостной выход', protective_exit: 'Выход по защитной заявке',
  control_exit: 'Ручное закрытие', active: 'Активна', canceled: 'Отменена', expired: 'Истекла', executed: 'Сработала',
  cancel_protection: 'Отмена защиты', cancel_orders: 'Отмена заявки', sl: 'Стоп-лосс', tp: 'Тейк-профит',
  close_position_changed: 'Позиция или защита изменилась. Заявка по запросу не отправлена.',
  close_already_requested: 'Закрытие этого цикла уже запрошено.',
  broker_read_unavailable: 'Не удалось сверить данные брокера.',
  protection_unconfirmed: 'Платформа не подтвердила защиту позиции.',
  broker_check_failed: 'Сверка с брокером не подтверждена.',
  broker_reconciliation_required: 'Брокер ещё не подтвердил результат операции.',
  mcp_order_submission_unconfirmed: 'MCP не вернул ID заявки. Проверьте заявку и позицию в песочнице.',
  broker_source_mismatch: 'Для сверки выберите транспорт, которым начат торговый цикл.',
  broker_position_mismatch: 'Объём позиции расходится с подтверждёнными исполнениями.',
  executor_busy: 'Исполнитель сверяет другую операцию по счёту.',
  autonomous_protection_unavailable: 'Автономный вход заблокирован. Безопасная связка стопов в песочнице не подтверждена.',
  entries_paused: 'Новые входы на паузе.', entry_pause_changed: 'Решение сформировано до изменения паузы.',
  start_accepted: 'Команда запуска принята.', stop_accepted: 'Команда остановки принята.',
  restart_accepted: 'Команда перезапуска принята.', ignored: 'Команда уже выполняется или недоступна.',
  forbidden: 'Сессия истекла. Обновите страницу.', journal_error: 'Не удалось записать журнал команд.',
  settings_saved: 'Настройки сохранены.', pause_saved: 'Команда паузы сохранена.',
  settings_unavailable: 'Не удалось прочитать настройки. Проверьте локальный профиль.',
  settings_conflict: 'Настройки изменились в другой вкладке. Откройте сохранённую версию.',
  mode_requires_stop: 'Сначала остановите обработчик.',
  account_unconfirmed: 'Не удалось подтвердить состояние счёта у брокера.',
  account_has_position: 'На счёте есть открытая позиция.',
  account_has_operations: 'На счёте есть незавершённая операция.',
  invalid_channels: 'Каналы: укажите уникальные имена Telegram без ссылки, по одному на строку.',
  invalid_models: 'Модели: проверьте идентификаторы моделей указанных ролей.',
  invalid_aggregator: 'Выберите OpenRouter или Groq.',
  invalid_groq_credentials: 'Заполните groq.api_key в .local/credentials.toml и сохраните настройки ещё раз.',
  invalid_contracts: 'Контракты: проверьте пары UID и идентификатор компании.',
  invalid_mode: 'Выберите допустимый режим.',
  invalid_request: 'Сервер отклонил данные команды.',
  archive_unavailable: 'Архив временно недоступен.',
  configuration: 'Проверьте конфигурацию приложения.', directory: 'Не удалось открыть каталог данных.',
  directory_busy: 'Этот каталог уже занят другим обработчиком.', broker: 'Проверьте подключение, счёт и контракты.',
  sources: 'Проверьте подключение и локальную авторизацию Telegram.',
  worker: 'Процесс завершился. Запустите обработчик повторно.',
  worker_start: 'Не удалось запустить процесс обработчика.',
};
export const label = value => Object.hasOwn(labels, value) ? labels[value] : value || 'Неизвестно';
