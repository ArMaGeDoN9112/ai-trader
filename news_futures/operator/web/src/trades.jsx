import { ArrowLeft, ArrowUpRight, BookOpen, Layers3, ShieldCheck, Workflow } from 'lucide-react';
import { contractName, label, query, route, time, useResource } from './api';
import { Badge, Empty, Evidence, Facts, Notice, Panel, SourceLinks, Step } from './ui';

const lots = value => value == null ? '—' : value + ' лот.';
const price = value => value == null ? '—' : value + ' пт.';

export function Trades({ cycle, news, refresh }) {
  const { data, error } = useResource(cycle ? query('/api/trades', { cycle }) : '/api/trades', refresh, 2500);
  if (cycle) return <div className="stack">
    <div className="detail-nav"><a href="#trades"><ArrowLeft size={17} /> Все сделки</a></div>
    {error && <Notice error>{error === 'not_found' ? 'Торговый цикл не найден.' : label(error)}</Notice>}
    <Panel title={data ? contractName(news, data.contract_id) : 'Торговый цикл'} icon={Layers3}
      hint={data && [label(data.mode), data.account_id, label(data.side)].join(' · ')} actions={data && <Badge value={data.status} />}>
      {data && <>
        <Facts rows={[['Причина входа', data.entry_reason || 'Не сохранена'], ['Причина выхода', data.exit_reason ? label(data.exit_reason) : '—'],
          ['Защита по журналу', label(data.protection)], ['Последнее подтверждённое исполнение', time(data.confirmed_at)],
          ['Запрошенный объём входа', lots(data.requested_lots)]]} />
        {data.reason && <Notice>{label(data.reason)}</Notice>}
        <SourceLinks data={data.entry_sources} />
        <div className="actions card-actions">{data.traces.map(item => <a key={item.revision}
          className="button secondary" href={route('trace', item.revision, route('trades', cycle))}>
          <Workflow size={16} />{item.kind === 'entry' ? 'Трасса входа' : 'Трасса новостного выхода'}
        </a>)}</div>
      </>}
      {!data && !error && <Empty>Загрузка торгового цикла…</Empty>}
    </Panel>
    {data && <>
      <Panel title="Заявки и исполнения" hint="Средняя цена и объём показаны по подтверждениям брокера" icon={BookOpen}>
        <OrderTable orders={data.orders} />
      </Panel>
      {data.protection_orders.length > 0 && <Panel title="Защитные заявки" hint="Состояние по последней записи сверки" icon={ShieldCheck}>
        <div className="list">{data.protection_orders.map(item => <div className="list-row" key={item.id}>
          <span className="row-body"><strong>{label(item.kind)}</strong>
            <small>{lots(item.lots)} · Уровень {price(item.trigger_points)}</small></span>
          <Badge value={item.status} />
        </div>)}</div>
      </Panel>}
      <Panel title="Журнал сверки" icon={Workflow}>
        <ul className="timeline">{data.events.map((item, index) => <Step key={item.seq || index}
          title={label(item.kind)} at={time(item.at)}><Evidence value={item.details} /></Step>)}</ul>
        {!data.events.length && <Empty>Операции пока не сохранены.</Empty>}
      </Panel>
    </>}
  </div>;
  return <Panel title="Торговые циклы" hint="Позиции, которыми управляло приложение, и попытки входа" icon={Layers3}>
    {error && <Notice error>{label(error)}</Notice>}
    <div className="list">{data?.trades.map(item => <a key={item.id} className="list-row" href={route('trades', item.id)}>
      <span className="row-icon"><Layers3 size={17} /></span><span className="row-body">
        <strong>{contractName(news, item.contract_id)} · {label(item.side)}</strong>
        <small>{label(item.mode)} · Счёт {item.account_id} · {time(item.at)}</small>
        <small>Вход {lots(item.entry_lots)} · Средняя {price(item.entry_price)} · Выход {lots(item.exit_lots)} · Средняя {price(item.exit_price)}</small>
      </span><Badge value={item.status} /><ArrowUpRight className="row-arrow" size={16} />
    </a>)}</div>
    {!data?.trades.length && !error && <Empty>{data ? 'В журнале пока нет торговых циклов.' : 'Загрузка сделок…'}</Empty>}
  </Panel>;
}

function OrderTable({ orders }) {
  if (!orders.length) return <Empty>Заявки ещё не сохранены.</Empty>;
  return <div className="table-wrap" tabIndex={0} aria-label="Таблица заявок и исполнений">
    <table><thead><tr><th>Действие</th><th>Статус</th><th>Запрошено</th><th>Исполнено</th><th>Средняя цена</th></tr></thead>
      <tbody>{orders.map(item => <tr key={item.id}>
        <td><strong>{label(item.action)}</strong>{item.side && <small>{item.side === 'buy' ? 'Покупка' : 'Продажа'}</small>}</td>
        <td><Badge value={item.status} /></td><td>{lots(item.requested)}</td><td>{lots(item.filled)}</td><td>{price(item.price)}</td>
      </tr>)}</tbody>
    </table>
  </div>;
}
