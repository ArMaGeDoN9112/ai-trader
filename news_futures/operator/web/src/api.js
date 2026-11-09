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
  unconfirmed: 'Не подтверждена', long: 'Лонг', short: 'Шорт', protected: 'Защита подтверждена',
  processing: 'Не завершено', saved: 'Сохранено', completed: 'Завершено', continue: 'Пройдено',
  skipped: 'Пропущено', rejected: 'Отклонено', blocked: 'Заблокировано', approved: 'Одобрено',
  no_action: 'Без торгового действия', approved_for_checks: 'Одобрено для проверки',
  repeat: 'Повтор', update: 'Обновление', uncertain_update: 'Спорное обновление',
  uncertain_match: 'Возможное совпадение', unmatched: 'Отдельное событие', model_error: 'Ошибка модели',
  event_model_error: 'Ошибка модели сопоставления события', runtime_profile_changed: 'Рабочий профиль изменился',
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
