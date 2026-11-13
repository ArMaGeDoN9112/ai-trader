import { useState } from 'react';
import { Bot, Radio, Save, SlidersHorizontal } from 'lucide-react';
import { label, time } from './api';
import { Empty, Facts, Notice, Panel } from './ui';

const roles = [['jev', 'Привязка к компаниям · OpenRouter'], ['analyst', 'Аналитик'], ['critic', 'Критик']];
const aggregators = { openrouter: 'OpenRouter', groq: 'Groq' };
const lines = value => value.split('\n').map(line => line.trim()).filter(Boolean);

export function Settings({ settings, status, online, busy, command, error }) {
  const [draft, setDraft] = useState(null);
  if (!settings) return <Panel title="Конфигурация" icon={SlidersHorizontal}><Empty>Загрузка настроек…</Empty></Panel>;
  const { saved, active, changed_at, models_at, profile_at } = settings;
  const values = draft || { mode: saved.mode, channels: saved.channels.join('\n'),
    analysis_strategy: saved.analysis_strategy || 'fixed',
    aggregator: saved.aggregator, models: saved.models, groq_models: saved.groq_models };
  const set = (field, value) => setDraft({ ...values, base: draft?.base || saved, [field]: value });
  const restart = ['channels', 'analysis_strategy']
    .some(key => JSON.stringify(active[key]) !== JSON.stringify(saved[key]));
  const activeModels = roles.map(([id, name]) => [name, <span className="mono">{active.models[id]}</span>]);
  return <form className="stack" onSubmit={async event => {
    event.preventDefault();
    const result = await command('settings', { base: draft?.base || saved, mode: values.mode, channels: lines(values.channels),
      analysis_strategy: values.analysis_strategy,
      aggregator: values.aggregator, models: values.models, groq_models: values.groq_models });
    if (result) setDraft(null);
  }}>
    <div className="settings-grid">
      <Panel title="Режим работы" icon={SlidersHorizontal}>
        <Facts rows={[['Счёт', status.account_id || '—'], ['Активный режим', label(active.mode)],
          ['Маршрут анализа', label(active.analysis_strategy || 'fixed')],
          ['Применено', time(profile_at)]]} />
        <label className="field">Сохранённый режим<select value={values.mode}
          disabled={!['stopped', 'failed'].includes(status.worker) || !online || busy}
          onChange={event => set('mode', event.target.value)}>
          <option value="observation">Наблюдение</option><option value="sandbox">Песочница</option>
        </select></label>
        <p className="muted">Смена режима требует остановки обработчика и сверки свободного счёта.</p>
        <label className="field">Маршрут анализа<select id="analysis-strategy" value={values.analysis_strategy}
          onChange={event => set('analysis_strategy', event.target.value)}>
          <option value="fixed">Фиксированный маршрут</option>
          <option value="tool_agent">{label('tool_agent')}</option>
        </select></label>
        <p className="muted">Маршрут применяется при запуске обработчика.</p>
        {values.mode === 'sandbox' && values.analysis_strategy === 'tool_agent' &&
          <p className="muted">Команда готовит предложения для ручной отправки.</p>}
      </Panel>
      <Panel title="Источники Telegram" icon={Radio}>
        <Facts rows={[['Активные каналы', active.channels.map(name => '@' + name).join(', ') || '—']]} />
        <label className="field">Сохранённые каналы<textarea rows={5} spellCheck={false} aria-invalid={error === 'invalid_channels'}
          value={values.channels} onChange={event => set('channels', event.target.value)} /></label>
        <p className="muted">По одному имени канала в строке. Изменения вступают в силу после перезапуска.</p>
        {restart && <Notice>Настройки профиля сохранены. Для применения перезапустите обработчик.</Notice>}
      </Panel>
    </div>
    <Panel title="Модели ролей" icon={Bot} hint="Агрегатор и модели применяются со следующей редакции без перезапуска">
      <div className="settings-grid">
        <div><h3 className="subheading">Применённые модели</h3><Facts rows={[
          ['Агрегатор чат-моделей', models_at ? aggregators[active.aggregator] : '—'],
          ...activeModels, ['Применено', time(models_at)],
        ]} /></div>
        <div className="stack compact">
          <label className="field">Агрегатор чат-моделей<select id="chat-aggregator" value={values.aggregator}
            aria-invalid={['invalid_aggregator', 'invalid_groq_credentials'].includes(error)}
            onChange={event => set('aggregator', event.target.value)}>
            {Object.entries(aggregators).map(([id, name]) => <option key={id} value={id}>{name}</option>)}
          </select></label>
          {roles.map(([id, name]) => {
            const field = id !== 'jev' && values.aggregator === 'groq' ? 'groq_models' : 'models';
            return <label className="field" key={id}>{name}
              <input spellCheck={false} value={values[field][id]} required aria-invalid={error === 'invalid_models'} onChange={event =>
                set(field, { ...values[field], [id]: event.target.value.trim() })} />
            </label>;
          })}
        </div>
      </div>
    </Panel>
    <div className="save-bar"><small>Последнее сохранение · {time(changed_at)}</small>
      {draft && <button className="button secondary" type="button" onClick={() => setDraft(null)}>Сбросить правки</button>}
      <button className="button primary" disabled={busy || !online || !draft}><Save size={16} />Сохранить настройки</button>
    </div>
  </form>;
}
