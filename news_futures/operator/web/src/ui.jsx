import { Activity, ArrowDownRight, ArrowUpRight, ChevronRight, CircleAlert, FileText, Inbox } from 'lucide-react';
import { companyName, contractName, label, route, time } from './api';

export function Badge({ value, children, tone }) {
  const color = tone || ({
    connected: 'green', running: 'green', confirmed: 'green', open: 'green', approved: 'green',
    protected: 'green', sandbox: 'purple', observation: 'purple', failed: 'red', rejected: 'red',
    unavailable: 'red', blocked: 'amber', paused: 'amber', processing: 'amber', starting: 'amber',
    stopping: 'amber', unconfirmed: 'amber',
  })[value] || 'neutral';
  return <span className={'badge ' + color}><i />{children || label(value)}</span>;
}

export function Panel({ title, hint, icon: Icon = Activity, actions, children, className = '' }) {
  return <section className={'panel ' + className}>
    <div className="panel-heading">
      <div className="panel-title"><span className="tile-icon"><Icon size={17} /></span>
        <div><h2>{title}</h2>{hint && <p>{hint}</p>}</div>
      </div>
      {actions}
    </div>
    {children}
  </section>;
}

export function Empty({ children, icon: Icon = Inbox }) {
  return <div className="empty"><Icon size={20} /><p>{children}</p></div>;
}

export function Notice({ children, error = false }) {
  return <p className={'notice' + (error ? ' danger' : '')} role={error ? 'alert' : 'status'}>
    <CircleAlert size={17} /> <span>{children}</span>
  </p>;
}

export function Facts({ rows }) {
  return <dl className="facts">{rows.map(([name, value]) => <div key={name}><dt>{name}</dt><dd>{value}</dd></div>)}</dl>;
}

export function Filter({ id, title, options, empty, ...props }) {
  return <label className="field" htmlFor={id}>{title}
    <select id={id} {...props}>
      <option value="">{empty}</option>
      {options.map(([value, name]) => <option key={value} value={value}>{name}</option>)}
    </select>
  </label>;
}

export function DecisionList({ id, rows, news, from }) {
  return <div id={id} className="list">
    {rows.length === 0 && <Empty icon={FileText}>Нет решений по выбранным условиям.</Empty>}
    {rows.map(row => <a key={row.revision} className="list-row" href={route('trace', row.revision, from)}>
      <span className="row-icon"><FileText size={17} /></span>
      <span className="row-body"><strong>{row.title || 'Открыть трассу'}</strong>
        <small>{[...row.companies.map(item => companyName(news, item)),
          ...row.contracts.map(item => contractName(news, item)), time(row.at)].join(' · ')}</small>
        {row.reason && <small>{label(row.reason)}</small>}
      </span>
      <Badge value={row.status} /><ChevronRight className="row-arrow" size={17} />
    </a>)}
  </div>;
}

export function SourceLinks({ data }) {
  const links = new Set();
  function visit(value) {
    if (!value || typeof value !== 'object') return;
    for (const [key, item] of Object.entries(value)) {
      if (['url', 'publication_url', 'source_url'].includes(key) && typeof item === 'string') {
        try {
          const url = new URL(item);
          if (['http:', 'https:'].includes(url.protocol) && !url.username && !url.password &&
              ![...url.searchParams.keys()].some(name => /token|secret|password|api.?key|session|auth/i.test(name))) {
            links.add(url.href);
          }
        } catch { /* Archived URLs can be incomplete. */ }
      } else {
        visit(item);
      }
    }
  }
  visit(data);
  if (!links.size) return null;
  return <ul className="sources-list">{[...links].map(url =>
    <li key={url}><a href={url} target="_blank" rel="noopener noreferrer">{url}<ArrowUpRight size={14} /></a></li>)}</ul>;
}

export function Evidence({ value, publication = false }) {
  if (!value || (typeof value === 'object' && !Object.keys(value).length)) return <p className="muted">Детали не сохранены.</p>;
  if (publication && value.text) return <p className="preserve">{value.text}</p>;
  return <pre>{JSON.stringify(value, null, 2)}</pre>;
}

export function Step({ title, at, status, children, id }) {
  return <li className="step" data-id={id}>
    <details><summary><span className="step-label"><strong>{title}</strong><small>{at}</small></span>
      {status && <Badge value={status} />}<ChevronRight className="step-arrow" size={17} />
    </summary><div className="step-content">{children}</div></details>
  </li>;
}

export function PositionValue({ record, news }) {
  if (!record) return 'Неизвестно';
  if (!record.position) return 'Позиции нет';
  const { contract_id: id, side, quantity } = record.position;
  const Direction = side === 'long' ? ArrowUpRight : ArrowDownRight;
  return <span className="position-value"><Direction size={16} />{contractName(news, id)} · {label(side)} · {quantity} лот.</span>;
}
