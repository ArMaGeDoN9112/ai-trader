import { useState } from 'react';
import { FlaskConical, ListChecks, Send } from 'lucide-react';
import { emptyNews, label, useResource } from './api';
import { DecisionList, Empty, Notice, Panel } from './ui';

export function Testing({ online, busy, command, refresh, worker }) {
  const [text, setText] = useState('');
  const { data, error } = useResource('/api/testing', refresh, 2000);
  return <div className="stack">
    <Panel title="Новая тестовая новость" icon={FlaskConical} hint="Пройдёт через выбранный маршрут анализа без отправки заявок">
      <form className="stack" onSubmit={async event => {
        event.preventDefault();
        if (await command('testing', { text: text.trim() })) setText('');
      }}>
        <label className="field">Текст новости
          <textarea id="test-news-text" rows={7} value={text} maxLength={4000} required
            placeholder="Вставьте заголовок и текст публикации" onChange={event => setText(event.target.value)} />
        </label>
        <div className="actions">
          <button className="button primary" type="submit" disabled={busy || !online || !text.trim()}>
            <Send size={16} />Добавить в очередь
          </button>
        </div>
        {online && worker !== 'running' && <Notice>Запустите обработчик на вкладке «Обзор», чтобы разобрать новости из очереди.</Notice>}
      </form>
    </Panel>
    <Panel title="Тестовые прогоны" icon={ListChecks}>
      {error && <Notice error>{label(error)}</Notice>}
      {data?.decisions.length > 0 ? <DecisionList id="test-news-list" rows={data.decisions} news={emptyNews} from="testing" />
        : data ? <Empty>Тестовых новостей пока нет.</Empty> : !error && <Empty>Загрузка прогонов…</Empty>}
    </Panel>
  </div>;
}
