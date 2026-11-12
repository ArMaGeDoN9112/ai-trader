import { useState } from 'react';
import { ArrowLeft, ChevronRight, FileText, Layers3, Search, Workflow } from 'lucide-react';
import { companyName, contractName, contractOptions, label, query, route, time, useResource } from './api';
import { Badge, DecisionList, Empty, Evidence, Facts, Filter, Notice, Panel, PositionValue, SourceLinks, Step } from './ui';

export function Decisions({ news }) {
  const [filters, set] = useState({ company: '', contract: '', result: '', from: '', to: '' });
  function props(name) {
    return { id: 'decision-' + name, value: filters[name],
      onChange: event => set({ ...filters, [name]: event.target.value, ...(name === 'company' ? { contract: '' } : {}) }) };
  }
  const { company, contract, result, from, to } = filters;
  const companies = Object.keys(news.companies).map(id => [id, companyName(news, id)]).sort((a, b) => a[1].localeCompare(b[1], 'ru'));
  const results = [...new Set(['processing', 'skipped', 'blocked', 'rejected', 'failed',
    'no_action', 'approved_for_checks', 'protected', 'closed', ...news.decisions.map(row => row.status)])];
  const until = to ? new Date(to + 'T00:00:00') : null;
  if (until) until.setDate(until.getDate() + 1);
  const rows = news.decisions.filter(row => {
    const stamp = Date.parse(row.at);
    return (!company || row.companies.includes(company)) && (!contract || row.contracts.includes(contract)) &&
      (!result || result === row.status) && (!from || stamp >= Date.parse(from + 'T00:00:00')) && (!until || stamp < until);
  });
  return <div id="decisions-screen"><Panel title="Журнал решений" icon={FileText}>
    <div className="filters">
      <Filter title="Компания" options={companies} empty="Все компании" {...props('company')} />
      <Filter title="Серия" options={contractOptions(news, company)} empty="Все серии" {...props('contract')} />
      <Filter title="Результат" options={results.map(value => [value, label(value)])} empty="Все результаты" {...props('result')} />
      <label className="field">С даты<input type="date" {...props('from')} /></label>
      <label className="field">По дату<input type="date" {...props('to')} /></label>
    </div>
    <DecisionList id="decisions" rows={rows} news={news} from="decisions" />
  </Panel></div>;
}

function tickerMatches(rows, news, term) {
  return rows.filter(item => [item.ticker, item.asset, item.uid, ...(news.companies[item.company_id]?.tickers || [])]
    .some(value => value.toUpperCase().includes(term)));
}

const priceFormat = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 9 });
const expiryFormat = new Intl.DateTimeFormat('ru-RU', { dateStyle: 'medium', timeZone: 'Europe/Moscow' });

function MarketList({ rows, news }) {
  return <div className="list" id="active-tickers">{rows.map(item => {
    const expiry = Date.parse(item.expiration);
    const content = <span className="row-body">
      <strong>{item.ticker} <span className="muted">· {item.asset}</span></strong>
      <small>{item.price == null ? 'Цена недоступна' : `Последняя цена ${priceFormat.format(item.price)} п. · ${time(item.quote_at)}`}</small>
      {Number.isFinite(expiry) && <small>Экспирация {expiryFormat.format(expiry)}</small>}
    </span>;
    return news.companies[item.company_id]
      ? <a className="list-row" key={item.uid} href={route('tickers', item.company_id, item.uid)}>{content}<ChevronRight className="row-arrow" size={17} /></a>
      : <div className="list-row" key={item.uid}>{content}</div>;
  })}</div>;
}

function relation(row) {
  return !row.binding ? 'Материал того же события' : row.binding.kind === 'direct' ? 'Прямая связь' : 'Гипотеза';
}

export function Tickers({ news, company, contract = '', broker, refresh }) {
  const market = useResource('/api/tickers', refresh, 25000);
  const [term, setTerm] = useState('');
  const active = market.error ? [] : market.data?.tickers || [];
  const matches = term ? tickerMatches(active, news, term) : active;
  const series = active.filter(item => item.company_id === company);
  const known = Object.hasOwn(news.companies, company);
  const events = Map.groupBy(news.publications, row => row.event_id || row.revision);
  const relevant = record => record?.position && (!contract || contract === record.position.contract_id)
    && news.contracts[record.position.contract_id]?.company_id === company;
  const current = broker?.current;
  const last = broker?.last_confirmed;
  const from = route('tickers', company, contract);
  return <div id="tickers-screen" className="stack">
    <Panel title="Поиск по рынку" icon={Search}>
      <form className="filters flush" id="ticker-form" onSubmit={event => {
        event.preventDefault();
        const value = new FormData(event.currentTarget).get('ticker').trim().toUpperCase();
        const found = tickerMatches(active, news, value);
        const exact = found.filter(item => item.ticker.toUpperCase() === value || item.uid.toUpperCase() === value);
        const owners = new Set(found.map(item => item.company_id));
        if (exact.length === 1 && exact[0].company_id) {
          location.hash = route('tickers', exact[0].company_id, exact[0].uid);
          setTerm('');
        } else if (value && owners.size === 1 && found[0].company_id) {
          location.hash = route('tickers', found[0].company_id);
          setTerm('');
        } else {
          location.hash = route('tickers');
          setTerm(value);
        }
      }}>
        <label className="field grow">Тикер фьючерса или базовый актив
          <input id="ticker-search" name="ticker" placeholder="Например, SBER или BRZ6" autoComplete="off" />
        </label>
        <button className="button primary" type="submit" disabled={!market.data || Boolean(market.error)}><Search size={16} />Найти</button>
      </form>
      {market.error && <Notice error>Не удалось проверить торговый статус в Т‑Инвестициях.</Notice>}
    </Panel>
    {known ? <>
      <Panel title={<span id="company-name">{companyName(news, company)}</span>} icon={Layers3}
        hint={news.companies[company].tickers.join(' · ')}>
        <div className="filters">
          <Filter id="company-contract" title="Серия фьючерса" empty="Все серии" options={series.map(item => [item.uid, item.ticker])}
            value={contract} onChange={event => { location.hash = route('tickers', company, event.target.value); }} />
          <p className="muted">Фильтр применяется к решениям, заявкам и позиции.</p>
        </div>
        <MarketList rows={contract ? series.filter(item => item.uid === contract) : series} news={news} />
        <div className="inline-state" id="company-position">
          <strong>Позиция</strong>{current && !relevant(current) ? 'По выбранным сериям позиции нет' : <PositionValue record={current} news={news} />}
          {!current && relevant(last) && <small>Последнее подтверждение · <PositionValue record={last} news={news} /> · {time(last.checked_at)}</small>}
        </div>
      </Panel>
      <Panel title="Новостной контекст" icon={FileText} hint="События и публикации по всей компании">
        <div id="company-events" className="event-list">
          {events.size ? [...events.entries()].map(([id, rows]) => <div key={id} className="event">
            <h3>{label(rows[0].event_kind)} <span className="muted">· {rows.length} ред.</span></h3>
            <ul className="timeline">{rows.map(row => <Step key={row.revision} title={row.title || 'Публикация'}
              at={[time(row.at), relation(row), row.publication.edited_at ? 'Редакция' : 'Публикация'].join(' · ')}>
              <p className="preserve">{row.publication.text || 'Текст не сохранён.'}</p>
              {row.assessments.map(item => <p key={item.contract_id} className="assessment">
                Проверка аналитиком · {contractName(news, item.contract_id)} · {item.impact === 'verified' ? 'Влияние подтверждено' : 'Влияние не подтверждено'} · {item.reason}
              </p>)}
              <SourceLinks data={row.publication} />
              {row.has_trace && <a href={route('trace', row.revision, from)}>Открыть трассу <ChevronRight size={15} /></a>}
            </Step>)}</ul>
          </div>) : <Empty>Сохранённых публикаций пока нет.</Empty>}
        </div>
      </Panel>
      <Panel title="Решения по компании" icon={Workflow}>
        <DecisionList id="company-decisions" rows={news.decisions.filter(row =>
          row.companies.includes(company) && (!contract || row.contracts.includes(contract)))} news={news} from={from} />
      </Panel>
      <Panel title="Заявки по компании" icon={Layers3}>
        <div className="list">{news.orders.filter(row => !contract || row.contract_id === contract).map(row =>
          <div className="list-row" key={row.id || [row.at, row.contract_id, row.action].join(':')}>
            <span className="row-body"><strong>{contractName(news, row.contract_id)} · {label(row.action)}</strong><small>{time(row.at)}</small></span>
            <Badge value={row.status} />
          </div>)}</div>
        {!news.orders.some(row => !contract || row.contract_id === contract) && <Empty>Нет заявок по выбранным сериям.</Empty>}
      </Panel>
    </> : <Panel title="Активные фьючерсы" icon={Layers3}
      hint={market.data?.checked_at && !market.error ? 'Торговый статус проверен ' + time(market.data.checked_at) : undefined}>
      {matches.length > 0 ? <MarketList rows={matches} news={news} /> : !market.error && <Empty>
        {!market.data ? 'Проверяем торговый статус…' : !market.data.checked_at ? 'Каталог появится после запуска обработчика.'
          : term ? 'Среди активных фьючерсов совпадений нет.' : 'Сейчас нет фьючерсов с доступной подачей заявок.'}
      </Empty>}
    </Panel>}
  </div>;
}

const steps = {
  publication: 'Исходная публикация', source: 'Получение публикации',
  category: 'Категория новости', route: 'Привязки к компаниям', event: 'Сопоставление события',
  position: 'Проверка открытой позиции', snapshot: 'Подготовка контекста',
  analyst: 'Предложение аналитика', critic: 'Ответ критика',
  revision: 'Пересмотр аналитика', review: 'Проверка предложения', risk: 'Проверка риска',
  execution: 'Исполнитель', processing: 'Обработка',
  agent: 'Решение координатора', coordinator: 'Ход координатора',
  get_company_news: 'Поиск новостей компании', get_market_context: 'Проверка рынка',
  ask_analyst: 'Анализ компании', ask_critic: 'Проверка критиком', agent_tool: 'Вызов инструмента',
};

const agentSteps = new Set(['coordinator', 'get_company_news', 'get_market_context',
  'ask_analyst', 'ask_critic', 'agent_tool']);

function TechnicalDetails({ input, output }) {
  const [open, setOpen] = useState(false);
  return <details className="trace-technical" onToggle={event => setOpen(event.currentTarget.open)}>
    <summary>Технические детали (JSON)</summary>
    {open && <div className="step-content">
      {input && <><h3>Сохранённый вход</h3><Evidence value={input} /></>}
      {output && <><h3>Сохранённый ответ</h3><Evidence value={output} /></>}
    </div>}
  </details>;
}

function RoutingEvidence({ step, news }) {
  const { error, links, batches = [], actual_model, requested_model } = step.output || {};
  const groups = batches.filter(batch => batch.company_ids?.length);
  const firstFailed = groups.findIndex(batch => batch.error);
  const failure = error || groups[firstFailed]?.error;
  const finished = step.status === 'completed' || step.id === 'bindings';
  const failed = step.status === 'failed' || Boolean(failure);
  const model = actual_model || requested_model;
  const rows = [];
  if (groups.length) {
    rows.push(['Запросы без ошибок', groups.filter(batch => !batch.error).length + ' из ' + groups.length + ' отправленных']);
  }
  if (firstFailed >= 0) rows.push(['Номер запроса с ошибкой', firstFailed + 1]);
  if (model) rows.push([actual_model ? 'Модель OpenRouter' : 'Запрошенная модель OpenRouter', model]);

  return <>
    {failed ? <Notice error>{failure ? label(failure) + '. Проверка прервана.' : 'Проверка прервана.'}</Notice>
      : finished ? <p>{links?.length ? 'Найденные связи' : 'Связей с компаниями не найдено.'}</p>
        : <p>Проверка ещё не завершена.</p>}
    {!failed && links?.length > 0 && <ul className="routing-links">
      {links.map(link => {
        const company = step.input?.companies?.[link.company_id] || news.companies[link.company_id];
        const name = company?.names?.[0] || company?.tickers?.[0] || 'Компания из каталога';
        return <li key={link.company_id}><strong>{name}</strong> · {relation({ binding: link })}</li>;
      })}
    </ul>}
    {rows.length > 0 && <Facts rows={rows} />}
    <TechnicalDetails input={step.input} output={step.output} />
  </>;
}

function EventEvidence({ step }) {
  const { assessment, search, settings = {}, error, reason, actual_model, provider } = step.output || {};
  const rows = [];
  if (search) {
    rows.push(['Событий просмотрено', search.events_examined], ['Прошли предварительный отбор', search.events_ranked],
      ['Кандидатов для сравнения', search.candidates?.length ?? step.input?.candidates?.length ?? 0]);
  }
  const service = provider || ({ groq: 'Groq', openrouter: 'OpenRouter' })[settings.aggregator];
  if (service) rows.push(['Сервис модели', service]);
  if (actual_model || settings.model) rows.push([actual_model ? 'Модель' : 'Запрошенная модель', actual_model || settings.model]);
  if (['OpenAITimeoutError', 'APITimeoutError'].includes(error) && settings.timeout_ms) {
    rows.push(['Тайм-аут запроса', settings.timeout_ms / 1000 + ' с']);
  }
  const failure = step.status === 'model_error' || step.status === 'failed';
  return <>
    {failure ? <Notice error>{label(error || reason || step.status)}</Notice>
      : <p>{assessment?.reason || (step.status === 'unmatched' && search?.candidates?.length === 0
        ? 'В архиве не нашлось кандидатов для сравнения.' : label(step.status))}</p>}
    {rows.length > 0 && <Facts rows={rows} />}
    <TechnicalDetails input={step.input} output={step.output} />
  </>;
}

function AgentEvidence({ step, news }) {
  const { output, arguments: args, error, actual_model, requested_model, latency_ms } = step.output || {};
  const rows = [];
  const company = args?.company_id || output?.company_id;
  if (company) rows.push(['Компания', companyName(news, company)]);
  if (args?.query) rows.push(['Поисковый запрос', args.query]);
  if (output?.news) rows.push(['Публикаций найдено', output.news.length]);
  if (output?.instrument) rows.push(['Контракт', contractName(news, output.instrument.contract_id)],
    ['Последняя цена', output.instrument.market.last_price], ['Котировка на', time(output.instrument.market.observed_at)]);
  if (output?.tool) rows.push(['Следующий шаг', steps[output.tool] || 'Неизвестный инструмент']);
  if (output?.action) rows.push(['Предложение', label(output.action)]);
  if (output?.missing_data?.length) rows.push(['Недостающие данные', output.missing_data.join(' · ')]);
  if (typeof output?.approved === 'boolean') rows.push(['Заключение', label(output.approved ? 'approved' : 'rejected')]);
  if (actual_model || requested_model) rows.push(['Модель', actual_model || requested_model]);
  if (latency_ms != null) rows.push(['Время вызова', (latency_ms / 1000).toLocaleString('ru-RU') + ' с']);
  return <>
    {error && <Notice error>{label(error)}</Notice>}
    {output?.reason && <p className="preserve">{output.reason}</p>}
    {output?.objections?.length > 0 && <ul className="routing-links">
      {output.objections.map((objection, index) => <li key={index}>{objection}</li>)}
    </ul>}
    {rows.length > 0 && <Facts rows={rows} />}
    <SourceLinks data={output?.news} />
    <TechnicalDetails input={step.input || args} output={step.output} />
  </>;
}

function TraceEvidence({ step, news }) {
  if (step.id === 'outcome') {
    const { proposal, critic_status } = step.output || {};
    return <>
      <p>{label(step.output?.reason || step.status)}</p>
      {proposal && <Facts rows={[
        ['Контракт', contractName(news, proposal.contract_id)], ['Направление', label(proposal.side)],
        ['Цена входа', proposal.entry_price], ['Стоп-лосс', proposal.stop_loss],
        ['Тейк-профит', proposal.take_profit], ['Проверка критиком', label(critic_status)],
      ]} />}
      {Object.keys(step.output || {}).some(key => key !== 'reason') && <TechnicalDetails output={step.output} />}
    </>;
  }
  if (agentSteps.has(step.kind)) return <AgentEvidence step={step} news={news} />;
  if (step.kind === 'publication') {
    return <><Evidence value={step.output} publication /><SourceLinks data={step.output} /></>;
  }
  if (step.kind === 'category') {
    const { status, categories = {}, selected = [], candidates = [], error } = step.output;
    return <>
      {status === 'failed' ? <Notice error>{label(error)}</Notice>
        : status === 'processing' ? <p>Модель определяет категории новости.</p>
          : <Facts rows={[
            ['Выбранные категории', selected.map(id => categories[id]).join(', ') || 'Нет подходящей категории'],
            ['Компаний для проверки', candidates.length],
          ]} />}
      <TechnicalDetails output={step.output} />
    </>;
  }
  if (step.kind === 'route') return <RoutingEvidence step={step} news={news} />;
  if (step.kind === 'event') return <EventEvidence step={step} />;
  if (step.kind === 'position') return step.output?.reason
    ? <p>{label(step.output.reason)}</p>
    : step.status === 'completed' ? <p>Счёт свободен. Новость передана на анализ.</p> : null;
  return <>
    <h3>Сохранённый вход</h3><Evidence value={step.input} />
    <h3>Сохранённый ответ</h3><Evidence value={step.output} />
    <SourceLinks data={[step.input, step.output]} />
  </>;
}

export function Trace({ revision, from, news, refresh }) {
  const { data, error } = useResource(revision ? query('/api/trace', { revision }) : null, refresh, 2000);
  const eventError = data?.reason === 'event_model_error' && data.steps.find(step => step.id === 'event')?.output?.error;
  return <div className="stack">
    <div className="detail-nav"><a href={from?.startsWith('#tickers/') || from?.startsWith('#trades/') ? from :
      from === 'testing' ? '#testing' : from === 'overview' ? '#overview' : '#decisions'}><ArrowLeft size={17} /> Назад</a>
      {data?.cycle_id && <a href={route('trades', data.cycle_id)}>Торговый цикл <ChevronRight size={17} /></a>}
    </div>
    {error && <Notice error>{error === 'not_found' ? 'Трасса не найдена.' : label(error)}</Notice>}
    <Panel title={data?.title || 'Ход обработки публикации'} icon={Workflow} actions={data && <Badge value={data.status} />}
      hint={data && [...data.companies.map(id => companyName(news, id)), time(data.at)].join(' · ')}>
      {data?.testing && <div className="agent-trace-summary"><Badge value="testing_publication" tone="purple" /></div>}
      {data?.analysis_strategy === 'tool_agent' && <div className="agent-trace-summary">
        <Badge value="tool_agent" tone="purple" />
        {data.critic_status && <span>Критик · {label(data.critic_status)}</span>}
      </div>}
      {data?.reason && <Notice error={data.status === 'failed'}>{label(eventError || data.reason)}</Notice>}
      <ul id="trace-steps" className="timeline">{data?.steps.map(step => <Step key={step.id} id={step.id}
        title={(step.id === 'outcome' && step.status !== 'processing' ? 'Итог · ' : '') + (steps[step.kind] || step.kind)}
        status={step.status} at={step.at ? time(step.at) : step.saved_at ? 'Сохранено ' + time(step.saved_at) : 'Время не сохранено'}>
        <TraceEvidence step={step} news={news} />
      </Step>)}</ul>
      {!data && !error && <Empty>Загрузка трассы…</Empty>}
    </Panel>
  </div>;
}
