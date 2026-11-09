import { useEffect, useRef, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, Boxes, ChartNoAxesCombined, CirclePause, FlaskConical, Gauge, GitBranch, Radio, Settings2 } from 'lucide-react';
import { emptyNews, label, query, request, useResource } from './api';
import { Badge, Notice } from './ui';
import { Overview } from './overview';
import { Decisions, Tickers, Trace } from './news';
import { Trades } from './trades';
import { Settings } from './settings';
import './style.css';

const pages = {
  overview: ['Обзор', 'Текущее состояние, контроль рисков и последние решения.', Gauge],
  tickers: ['Тикеры', 'Компании, серии фьючерсов и связанный новостной контекст.', Boxes],
  decisions: ['Решения', 'Путь от публикации до вывода агентов и проверки риска.', GitBranch],
  trades: ['Сделки', 'История торговых циклов и подтверждённые данные брокера.', ChartNoAxesCombined],
  settings: ['Настройки', 'Рабочий профиль, источники Telegram и модели ролей.', Settings2],
  trace: ['Трасса решения', 'Хронология обработки и сохранённые материалы.', GitBranch],
};

function readRoute() {
  try {
    const path = location.hash.slice(1).split('/').map(decodeURIComponent);
    if (Object.hasOwn(pages, path[0])) return path;
  } catch { /* Invalid URL fragments lead to the overview. */ }
  return ['overview'];
}

function App() {
  const [navigation, setNavigation] = useState(readRoute);
  const [refresh, setRefresh] = useState(0);
  const [notice, setNotice] = useState(null);
  const [busy, setBusy] = useState(false);
  const inFlight = useRef(false);
  const heading = useRef(null);
  const [screen, selection, detail] = navigation;
  useEffect(() => {
    function navigate() {
      setNavigation(readRoute());
      setNotice(null);
      window.scrollTo(0, 0);
      heading.current?.focus({ preventScroll: true });
    }
    window.addEventListener('hashchange', navigate);
    return () => window.removeEventListener('hashchange', navigate);
  }, []);

  const overview = useResource('/api/overview', refresh, 1500);
  const controls = useResource(screen === 'overview' || screen === 'settings' ? '/api/controls' : null, refresh, 1500);
  const news = useResource(screen === 'settings' ? null : screen === 'tickers' && selection
    ? query('/api/news', { company: selection }) : '/api/news', refresh, screen === 'trades' || screen === 'trace' ? 0 : 2000);
  const status = overview.data || {};
  const online = Boolean(overview.data && !overview.error);
  const data = news.data || emptyNews;

  async function command(name, body = {}) {
    if (inFlight.current) return false;
    inFlight.current = true;
    setBusy(true);
    setNotice(null);
    try {
      const value = await request('/api/' + name, body);
      setNotice({ text: label(value.result) });
      return true;
    } catch (error) {
      setNotice({ code: error.message, text: error.name === 'TypeError' || error.name === 'TimeoutError' ?
        'Ответ на команду не получен. Проверьте состояние в панели.' : label(error.message), error: true });
      return false;
    } finally {
      inFlight.current = false;
      setBusy(false);
      setRefresh(count => count + 1);
    }
  }

  const [title, description] = pages[screen];
  const activeScreen = screen === 'trace' ? 'decisions' : screen;
  return <div className="shell">
    <a href="#main-content" className="skip" onClick={event => { event.preventDefault(); heading.current?.focus(); }}>К содержимому</a>
    <aside className="sidebar">
      <a className="brand" href="#overview"><span className="brand-icon"><Activity size={23} /></span>
        <span><strong>News Futures</strong><small>Панель оператора</small></span>
      </a>
      <div className="nav-caption">РАБОЧАЯ ОБЛАСТЬ</div>
      <nav aria-label="Разделы панели">{Object.entries(pages).filter(([key]) => key !== 'trace').map(([key, [name, , Icon]]) =>
        <a key={key} href={'#' + key} aria-current={activeScreen === key ? 'page' : undefined}><Icon size={19} />{name}</a>)}
      </nav>
      <div className="sidebar-footer"><span className="terminal-dot" />
        <div><strong>Локальный доступ</strong><small className="mono">{location.hostname}</small></div>
      </div>
    </aside>
    <div className="workspace">
      <header className="topbar">
        <div className="top-label"><Activity size={16} />Терминал <span>/</span> <strong>{title}</strong></div>
        <div className="status-strip">
          <Badge value={status.mode}><FlaskConical size={14} />{label(status.mode)}</Badge>
          <Badge value={online ? status.worker : 'unknown'} />
          <Badge value={online && status.pause?.paused ? 'paused' : 'neutral'}>
            <CirclePause size={14} />{!online || !status.pause ? 'Пауза неизвестна' : status.pause.paused ? 'Входы на паузе' : 'Пауза снята'}
          </Badge>
        </div>
      </header>
      <main id="main-content" className="content">
        <div className="page-heading">
          <div><div className="eyebrow">NEWS FUTURES <span /> ТЕРМИНАЛ</div>
            <h1 id="page-title" ref={heading} tabIndex={-1}>{title}</h1><p>{description}</p>
          </div>
          <div className={'connection' + (online ? ' live' : '')} role="status">
            <Radio size={16} />{online ? 'Автообновление' : overview.error ? 'Связь потеряна' : 'Подключение'}
          </div>
        </div>
        {overview.error && <Notice error>{overview.error === 'forbidden' ? label('forbidden') : 'Связь с панелью потеряна. Показана последняя подтверждённая сверка.'}</Notice>}
        {(news.error || controls.error) && <Notice error>{label(news.error || controls.error)}</Notice>}
        {busy && <Notice>Ожидаем ответ на команду…</Notice>}
        {notice && <Notice error={notice.error}>{notice.text}</Notice>}
        {screen === 'overview' && <Overview status={status} news={data} online={online} busy={busy} command={command}
          controls={controls.error ? undefined : controls.data} />}
        {screen === 'tickers' && <Tickers news={data} company={selection} contract={detail} loading={!news.data}
          broker={online ? status.broker : { last_confirmed: status.broker?.last_confirmed }} />}
        {screen === 'decisions' && <Decisions news={data} />}
        {screen === 'trace' && <Trace key={selection} revision={selection} from={detail} news={data} refresh={refresh} />}
        {screen === 'trades' && <Trades key={selection} cycle={selection} news={data} refresh={refresh} />}
        {screen === 'settings' && <Settings settings={controls.data?.settings} status={status} online={online && !controls.error}
          busy={busy} command={command} error={notice?.code} />}
        <footer className="workspace-footer"><span>NEWS FUTURES</span><span>Локальная панель оператора</span></footer>
      </main>
    </div>
  </div>;
}

createRoot(document.getElementById('root')).render(<App />);
