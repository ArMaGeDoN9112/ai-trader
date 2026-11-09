import { useState } from 'react';
import { Bot, Radio, Save, SlidersHorizontal } from 'lucide-react';
import { label, time } from './api';
import { Empty, Facts, Notice, Panel } from './ui';

const roles = [['jev', 'Jev · привязки'], ['matcher', 'Сопоставление событий'], ['analyst', 'Аналитик'], ['critic', 'Критик']];
const lines = value => value.split('\n').map(line => line.trim()).filter(Boolean);

export function Settings({ settings, status, online, busy, command, error }) {
  const [draft, setDraft] = useState(null);
  if (!settings) return <Panel title="Конфигурация" icon={SlidersHorizontal}><Empty>Загрузка настроек…</Empty></Panel>;
  const { saved, active, changed_at, models_at, profile_at } = settings;
  const values = draft || { mode: saved.mode, channels: saved.channels.join('\n'),
    contracts: Object.entries(saved.contracts).map(item => item.join(' = ')).join('\n'), models: saved.models };
  const set = (field, value) => setDraft({ ...values, base: draft?.base || saved, [field]: value });
  const restart = ['channels', 'contracts'].some(key => JSON.stringify(active[key]) !== JSON.stringify(saved[key]));
  const activeModels = roles.map(([id, name]) => [name, <span className="mono">{active.models[id]}</span>]);
  return <form className="stack" onSubmit={async event => {
    event.preventDefault();
    const contracts = lines(values.contracts).map(line => line.split('=').map(item => item.trim()));
    const result = await command('settings', { base: draft?.base || saved, mode: values.mode, channels: lines(values.channels),
      contracts, models: values.models });
    if (result) setDraft(null);
  }}>
    <div className="settings-grid">
      <Panel title="Режим и контракты" icon={SlidersHorizontal}>
        <Facts rows={[['Счёт', status.account_id || '—'], ['Активный режим', label(active.mode)],
          ['Активные контракты', Object.keys(active.contracts).join(', ')] , ['Применено', time(profile_at)]]} />
        <label className="field">Сохранённый режим<select value={values.mode}
          disabled={!['stopped', 'failed'].includes(status.worker) || !online || busy}
          onChange={event => set('mode', event.target.value)}>
          <option value="observation">Наблюдение</option><option value="sandbox">Песочница</option>
        </select></label>
        <p className="muted">Смена режима требует остановки обработчика и сверки свободного счёта.</p>
        <label className="field">Разрешённые контракты<textarea rows={4} spellCheck={false} aria-invalid={error === 'invalid_contracts'}
          placeholder="UID = компания" value={values.contracts} onChange={event => set('contracts', event.target.value)} /></label>
        <p className="muted">Одна пара «UID контракта = идентификатор компании» в строке.</p>
      </Panel>
      <Panel title="Источники Telegram" icon={Radio}>
        <Facts rows={[['Активные каналы', active.channels.map(name => '@' + name).join(', ') || '—']]} />
        <label className="field">Сохранённые каналы<textarea rows={5} spellCheck={false} aria-invalid={error === 'invalid_channels'}
          value={values.channels} onChange={event => set('channels', event.target.value)} /></label>
        <p className="muted">По одному имени канала в строке. Изменения вступают в силу после перезапуска.</p>
        {restart && <Notice>Настройки профиля сохранены. Для применения перезапустите обработчик.</Notice>}
      </Panel>
    </div>
    <Panel title="Модели ролей" icon={Bot} hint="Новая модель применяется при обработке следующей редакции">
      <div className="settings-grid">
        <div><h3 className="subheading">Применённые модели</h3><Facts rows={[...activeModels, ['Применено', time(models_at)]]} /></div>
        <div className="stack compact">{roles.map(([id, name]) => <label className="field" key={id}>{name}
          <input spellCheck={false} value={values.models[id]} required aria-invalid={error === 'invalid_models'} onChange={event =>
            set('models', { ...values.models, [id]: event.target.value.trim() })} />
        </label>)}</div>
      </div>
    </Panel>
    <div className="save-bar"><small>Последнее сохранение · {time(changed_at)}</small>
      {draft && <button className="button secondary" type="button" onClick={() => setDraft(null)}>Сбросить правки</button>}
      <button className="button primary" disabled={busy || !online || !draft}><Save size={16} />Сохранить настройки</button>
    </div>
  </form>;
}
