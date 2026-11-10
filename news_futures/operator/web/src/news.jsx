import { useState } from 'react';
import { ArrowLeft, ChevronRight, FileText, Layers3, Search, Workflow } from 'lucide-react';
import { companyName, contractName, contractOptions, label, query, route, time, useResource } from './api';
import { Badge, DecisionList, Empty, Evidence, Filter, Notice, Panel, PositionValue, SourceLinks, Step } from './ui';

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
  return <div id="decisions-screen"><Panel title="Журнал решений" icon={FileText} hint="Решения по публикациям с сохранённой привязкой к компании">
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

function tickerMatches(news, term) {
  const matches = Object.entries(news.contracts).filter(([id, item]) =>
    item.company_id && [item.ticker, id].some(value => value?.toUpperCase() === term))
    .map(([id, item]) => [item.company_id, id]);
  for (const [id, item] of Object.entries(news.companies)) {
    if (item.tickers.some(value => value.toUpperCase() === term) && !matches.some(([owner]) => owner === id)) matches.push([id, '']);
  }
  return matches;
}

function relation(row) {
  return !row.binding ? 'Материал того же события' : row.binding.kind === 'direct' ? 'Прямая связь' : 'Гипотеза';
}

export function Tickers({ news, company, contract = '', broker, loading }) {
  const [matches, setMatches] = useState(null);
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
        const term = new FormData(event.currentTarget).get('ticker').trim().toUpperCase();
        if (loading || !term) return;
        const found = tickerMatches(news, term);
        if (found.length === 1) location.hash = route('tickers', ...found[0]);
        setMatches(found.length === 1 ? null : found);
      }}>
        <label className="field grow">Тикер акции или фьючерса
          <input id="ticker-search" name="ticker" placeholder="Например, SBER или SRZ6" autoComplete="off" required />
        </label>
        <button className="button primary" type="submit" disabled={loading}><Search size={16} />Найти</button>
      </form>
      {matches?.length === 0 && <Empty>Тикер не найден в сохранённом каталоге и архиве.</Empty>}
      {matches?.length > 1 && <div className="list">{matches.map(([owner, id]) =>
        <a className="list-row" key={owner + id} href={route('tickers', owner, id)} onClick={() => setMatches(null)}>
          <span className="row-body">{companyName(news, owner)} {id && '· ' + contractName(news, id)}</span><ChevronRight size={17} />
        </a>)}</div>}
    </Panel>
    {known ? <>
      <Panel title={<span id="company-name">{companyName(news, company)}</span>} icon={Layers3}
        hint={news.companies[company].tickers.join(' · ')}>
        <div className="filters">
          <Filter id="company-contract" title="Серия фьючерса" empty="Все серии" options={contractOptions(news, company)}
            value={contract} onChange={event => { location.hash = route('tickers', company, event.target.value); }} />
          <p className="muted">Фильтр применяется к решениям, заявкам и позиции.</p>
        </div>
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
    </> : <Panel title="Компании в каталоге" icon={Layers3}><div className="list">
      {!Object.keys(news.companies).length && <Empty>В каталоге пока нет компаний.</Empty>}
      {Object.entries(news.companies).map(([id, item]) => <a className="list-row" href={route('tickers', id)} key={id}>
        <span className="row-body"><strong>{companyName(news, id)}</strong><small>{item.tickers.join(' · ')}</small></span><ChevronRight size={17} />
      </a>)}
    </div></Panel>}
  </div>;
}

const steps = {
  publication: 'Исходная публикация', route: 'Привязки к компаниям', event: 'Сопоставление события',
  snapshot: 'Подготовка контекста', analyst: 'Предложение аналитика', critic: 'Ответ критика',
  revision: 'Пересмотр аналитика', review: 'Проверка предложения', risk: 'Проверка риска',
  execution: 'Исполнитель', processing: 'Обработка',
};

export function Trace({ revision, from, news, refresh }) {
  const { data, error } = useResource(revision ? query('/api/trace', { revision }) : null, refresh, 2000);
  return <div className="stack">
    <div className="detail-nav"><a href={from?.startsWith('#tickers/') || from?.startsWith('#trades/') ? from :
      from === 'overview' ? '#overview' : '#decisions'}><ArrowLeft size={17} /> Назад</a>
      {data?.cycle_id && <a href={route('trades', data.cycle_id)}>Торговый цикл <ChevronRight size={17} /></a>}
    </div>
    {error && <Notice error>{error === 'not_found' ? 'Трасса не найдена.' : label(error)}</Notice>}
    <Panel title={data?.title || 'Ход обработки публикации'} icon={Workflow} actions={data && <Badge value={data.status} />}
      hint={data && [...data.companies.map(id => companyName(news, id)), time(data.at)].join(' · ')}>
      {data?.reason && <Notice>{label(data.reason)}</Notice>}
      <ul id="trace-steps" className="timeline">{data?.steps.map(step => <Step key={step.id} id={step.id}
        title={(step.id === 'outcome' && step.status !== 'processing' ? 'Итог · ' : '') + (steps[step.kind] || step.kind)}
        status={step.status} at={step.at ? time(step.at) : step.saved_at ? 'Сохранено ' + time(step.saved_at) : 'Время не сохранено'}>
        <h3>Сохранённый вход</h3><Evidence value={step.input} publication={step.kind === 'publication'} />
        <h3>Сохранённый ответ</h3><Evidence value={step.output} publication={step.kind === 'publication'} />
        <SourceLinks data={[step.input, step.output, data.publication]} />
      </Step>)}</ul>
      {!data && !error && <Empty>Загрузка трассы…</Empty>}
    </Panel>
  </div>;
}
