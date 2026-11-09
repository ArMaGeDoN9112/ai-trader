import { useRef, useState } from 'react';
import { Activity, ArrowRight, ArrowUpRight, CirclePause, Database, Layers3, Play, Radio, RotateCw, ShieldCheck, Square, Workflow } from 'lucide-react';
import { contractName, label, route, time } from './api';
import { Badge, DecisionList, Empty, Facts, Notice, Panel, PositionValue } from './ui';

function SignalArt() {
  return <svg className="signal-art" viewBox="0 0 400 226" fill="none" aria-hidden="true">
    <defs>
      <pattern id="grid" width="22" height="22" patternUnits="userSpaceOnUse"><path d="M 22 0 L 0 0 0 22" stroke="#a8a2ea" strokeOpacity=".16" /></pattern>
      <radialGradient id="fade"><stop stopColor="white" /><stop offset="1" stopColor="white" stopOpacity="0" /></radialGradient>
      <mask id="signal-mask"><rect width="400" height="226" fill="url(#fade)" /></mask>
    </defs>
    <g mask="url(#signal-mask)">
      <rect width="400" height="226" fill="url(#grid)" />
      <g stroke="#a8a2ea" strokeOpacity=".65">
        <circle cx="218" cy="113" r="86" /><circle cx="218" cy="113" r="64" strokeDasharray="2 5" />
        <circle cx="218" cy="113" r="42" /><circle cx="218" cy="113" r="21" />
        <path d="M218 14v198M119 113h198" strokeDasharray="3 5" />
      </g>
      <path d="M55 129h101l15-19 14 31 20-51 16 23h42l19-27h90" stroke="#a99eff" strokeWidth="2" strokeLinejoin="round" />
      <circle cx="218" cy="113" r="4" fill="#d6cdff" /><circle cx="282" cy="86" r="4" fill="#a99eff" />
    </g>
  </svg>;
}

function Metric({ title, value, caption, icon: Icon, tone = 'purple' }) {
  return <div className="metric">
    <div className="metric-top"><span>{title}</span><span className={'tile-icon ' + tone}><Icon size={17} /></span></div>
    <strong>{value}</strong><small>{caption}</small>
  </div>;
}

export function Overview({ status, news, online, busy, command, controls }) {
  const dialog = useRef(null);
  const [confirmation, setConfirmation] = useState(null);
  const { broker, worker, pause } = status;
  const current = online ? broker?.current : null;
  const last = broker?.last_confirmed;
  const position = current?.position;
  const id = position?.contract_id;
  const company = news.contracts[id]?.company_id || status.contracts?.[id];
  const actions = online ? status.allowed || [] : [];
  const channels = Object.entries(status.sources || {});
  const connected = online ? channels.filter(([, state]) => state === 'connected').length : 0;
  const nextStart = ['stopped', 'failed'].includes(worker) ? controls?.settings?.saved : null;
  const close = controls?.close;
  const canClose = online && controls && status.mode === 'sandbox' && worker === 'running' && position?.cycle_id && !busy
    && !(['queued', 'accepted'].includes(close?.status) && close.cycle_id === position.cycle_id);
  return <div className="stack" id="overview-screen">
    <section className="hero">
      <SignalArt />
      <div className="hero-content">
        <div className="kicker"><span /> УПРАВЛЕНИЕ СЕССИЕЙ</div>
        <h2>Обработчик новостей</h2>
        <p>Сбор публикаций, проверка решений и сверка с брокером.</p>
        <div className="actions">
          {actions.includes('start') || worker !== 'running' ? <button className="button primary" type="button" data-action="start"
            disabled={busy || !actions.includes('start')} onClick={() => command('start')}><Play size={16} />Запустить</button> : null}
          <button className="button secondary" type="button" data-action="stop"
            disabled={busy || !actions.includes('stop')} onClick={() => command('stop')}><Square size={15} />Остановить</button>
          <button className="button secondary" type="button" data-action="restart"
            disabled={busy || !actions.includes('restart')} onClick={() => command('restart')}><RotateCw size={16} />Перезапустить</button>
          <button className="button secondary" type="button" disabled={busy || !online || !controls}
            onClick={() => command('pause', { paused: !pause?.paused })}>
            {pause?.paused ? <Play size={16} /> : <CirclePause size={17} />}
            {pause?.paused ? 'Снять паузу' : 'Пауза новых входов'}
          </button>
        </div>
        {pause?.changed_at && <small className="hero-note">
          {pause.applied_at ? 'Команда паузы применена · ' + time(pause.applied_at) : 'Команда паузы ожидает применения'}
        </small>}
        {online && status.command && <p className="hero-note" role="status">{label(status.command)}</p>}
      </div>
    </section>
    {online && status.failure && <Notice error>{label(status.failure)} {status.exit_code != null && 'Код завершения ' + status.exit_code + '.'}</Notice>}
    {online && status.journal_error && <Notice error>{label('journal_error')}</Notice>}
    <div className="metrics">
      <Metric title="Обработчик" value={label(online ? worker : 'unknown')} caption={status.mode ? label(status.mode) : 'Рабочий режим'}
        icon={Activity} tone="purple" />
      <Metric title="Брокер" value={label(online ? broker?.state : 'unknown')}
        caption={last ? 'Сверка ' + time(last.checked_at) : 'Подтверждений пока нет'} icon={Database} tone="blue" />
      <Metric title="Позиция" value={!current ? 'Неизвестно' : position ? contractName(news, id) : 'Позиции нет'}
        caption={position ? label(position.side) + ' · ' + position.quantity + ' лот.' : 'Подтверждение брокера'} icon={Layers3} tone="green" />
      <Metric title="Источники" value={online && channels.length ? connected + ' / ' + channels.length : '—'}
        caption={online ? 'Каналы Telegram на связи' : 'Связь не подтверждена'} icon={Radio} tone="amber" />
    </div>
    <div className="overview-grid">
      <Panel title="Позиция и защита" icon={ShieldCheck} actions={<Badge value={current?.protection || 'unknown'} />}>
        <Facts rows={[
          ['Текущая позиция', <PositionValue record={current} news={news} />],
          ['Последняя сверка', time(last?.checked_at)],
          ['Последняя подтверждённая позиция', last ? <PositionValue record={last} news={news} /> : '—'],
          ['Последняя подтверждённая защита', last ? label(last.protection) : '—'],
        ]} />
        {online && broker?.reason && <Notice>{label(broker.reason)}</Notice>}
        {online && status.entry_block && <Notice>{label(status.entry_block)}</Notice>}
        {close && <Notice error={close.status === 'rejected'}>
          Ручное закрытие · {close.reason ? label(close.reason) : label(close.status)}
        </Notice>}
        <div className="actions card-actions">
          {company && <a className="button secondary" href={route('tickers', company, id)}>Карточка компании <ArrowUpRight size={15} /></a>}
          {position?.cycle_id && <a className="button secondary" href={route('trades', position.cycle_id)}>Торговый цикл <ArrowRight size={15} /></a>}
          {status.mode === 'sandbox' && position?.cycle_id && <button className="button destructive" type="button" disabled={!canClose}
            onClick={() => { setConfirmation({ ...position, request_id: crypto.randomUUID() }); dialog.current.showModal(); }}>
            Закрыть позицию
          </button>}
        </div>
      </Panel>
      <Panel title="Рабочий профиль" icon={Layers3} actions={<a className="text-link" href="#settings">Настройки <ArrowUpRight size={15} /></a>}>
        <Facts rows={[
          ['Счёт', <span className="mono">{status.account_id || '—'}</span>],
          [nextStart ? 'Контракты следующего запуска' : 'Разрешённые контракты',
            Object.keys(nextStart?.contracts || status.contracts || {}).map(item => contractName(news, item)).join(', ') || '—'],
        ]} />
        <h3 className="subheading">Источники Telegram</h3>
        <div className="list sources">{channels.length ? channels.map(([name, state]) => <div key={name} className="list-row">
          <span className="row-body">@{name}</span><Badge value={online ? state : 'unknown'} />
        </div>) : <Empty>Каналы пока недоступны.</Empty>}</div>
        {status.processing_error && <Notice error>
          Последняя ошибка обработки · {time(status.processing_error.at)} · {label(status.processing_error.reason)}
        </Notice>}
      </Panel>
    </div>
    <Panel title="Последние решения" hint="Последние публикации, связанные с компаниями" icon={Workflow}
      actions={<a className="text-link" href="#decisions">Все решения <ArrowRight size={16} /></a>}>
      <DecisionList id="recent" rows={news.decisions.slice(0, 5)} news={news} from="overview" />
    </Panel>
    <dialog ref={dialog} aria-labelledby="close-title" className="modal">
      <h2 id="close-title">Закрыть позицию?</h2>
      <p>Обработчик повторно сверит позицию и её защиту перед отправкой заявки.</p>
      {confirmation && <Facts rows={[['Контракт', contractName(news, confirmation.contract_id)],
        ['Сторона', label(confirmation.side)], ['Объём', confirmation.quantity + ' лот.']]} />}
      <form method="dialog" className="actions">
        <button className="button secondary" autoFocus>Отмена</button>
        <button className="button destructive" type="button" disabled={busy || !online} onClick={() => {
          dialog.current.close();
          command('close', confirmation);
        }}>Подтвердить закрытие</button>
      </form>
    </dialog>
  </div>;
}
