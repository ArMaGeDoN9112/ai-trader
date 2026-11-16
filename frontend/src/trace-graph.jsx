import { useState } from 'react';
import { Bot, ChevronRight, GitBranch, Wrench } from 'lucide-react';
import { label, time } from './api';
import { Badge, Empty, Panel, Step } from './ui';

const roleNames = { coordinator: 'Координатор', analyst: 'Аналитик', critic: 'Критик' };
const modelStages = new Set(['category', 'route', 'event', 'instrument', 'tool_selection']);
const tools = new Set(['get_company_news', 'get_market_context', 'agent_tool']);
const nodeHeight = 56;
const rowHeight = 84;
const inset = 14;

function nodeId(step) {
  if (step.id === 'outcome') return 'outcome';
  return step.kind === 'revision' ? 'analyst' : step.kind;
}

function traceGraph(steps) {
  const nodes = new Map();
  const edges = new Map();
  function addNode(id) {
    if (!nodes.has(id)) nodes.set(id, { id, index: nodes.size, steps: [] });
  }
  function addEdge(from, to, kind) {
    if (from === to) return;
    const key = from + '/' + to;
    const previous = edges.get(key);
    if (previous && kind === 'sequence') return;
    edges.set(key, { from, to, kind });
  }
  for (const [index, step] of steps.entries()) {
    const id = nodeId(step);
    addNode(id);
    nodes.get(id).steps.push(step);
    const next = steps[index + 1];
    const handoff = step.output?.handoff_to;
    if (Object.hasOwn(roleNames, handoff)) {
      addNode(handoff);
      addEdge(id, handoff, 'handoff');
    }
    if (next) {
      const nextId = nodeId(next);
      const requestedTool = step.output?.output?.tool;
      const toolRequest = (id === 'tool_selection' || id === 'analyst') && tools.has(nextId) && requestedTool &&
        requestedTool === (next.output?.tool || nextId);
      const toolReturn = tools.has(id) && (nextId === 'tool_selection' || nextId === 'analyst') && step.output?.tool_call_id;
      addEdge(id, nextId, toolRequest || toolReturn ? 'tool' : 'sequence');
    }
  }
  return { nodes, edges: [...edges.values()] };
}

function edgePath(from, to, lane) {
  const y = inset + from * rowHeight;
  const end = inset + to * rowHeight;
  if (to === from + 1) return `M 50 ${y + nodeHeight + 1} V ${end - 5}`;
  const back = from > to;
  const startX = back ? 15 : 85;
  const endX = back ? 14 : 86;
  const bend = back ? 7 - lane : 93 + lane;
  return `M ${startX} ${y + 35} C ${bend} ${y + 35}, ${bend} ${y + 35}, ${bend} ${y + 43}
    V ${end + 12} Q ${bend} ${end + 20}, ${endX} ${end + 20}`;
}

const edgeNames = { handoff: 'Передача задачи', tool: 'Вызов и ответ инструмента', sequence: 'Порядок шагов' };

export function TraceGraph({ steps, names, title, renderEvidence }) {
  const [selection, setSelection] = useState('');
  const { nodes, edges } = traceGraph(steps);
  const selected = nodes.get(selection) || nodes.get('coordinator') || nodes.get('analyst') || nodes.values().next().value;
  if (!selected) return null;
  const name = id => roleNames[id] || (id === 'outcome' ? 'Итог' : names[id] || id);
  const connections = edges.filter(edge => edge.from === selected.id || edge.to === selected.id);
  const height = nodes.size * rowHeight - (rowHeight - nodeHeight) + inset * 2;
  return <Panel title="Граф обработки" icon={GitBranch} className="trace-graph">
    <div className="trace-graph-layout">
      <div className="trace-graph-map" role="group" aria-label="Узлы графа">
        <div className="trace-graph-scroll">
          <div className="trace-graph-canvas" style={{ height }}>
            <svg className="trace-graph-edges" aria-hidden="true" viewBox={`0 0 100 ${height}`} preserveAspectRatio="none">
              <defs>
                {Object.keys(edgeNames).map(kind => <marker key={kind} id={`trace-arrow-${kind}`}
                  markerWidth="5" markerHeight="5" refX="4" refY="2.5" orient="auto">
                  <path className={`trace-arrow ${kind}`} d="M0 0 L5 2.5 L0 5 Z" />
                </marker>)}
              </defs>
              {edges.map(({ from, to, kind }, index) => <path key={from + '/' + to}
                className={`trace-edge ${kind} ${from === selected.id || to === selected.id ? 'related' : ''}`}
                d={edgePath(nodes.get(from).index, nodes.get(to).index, index % 5)}
                vectorEffect="non-scaling-stroke" markerEnd={`url(#trace-arrow-${kind})`} />)}
            </svg>
            {[...nodes.values()].map(node => {
              const Icon = tools.has(node.id) ? Wrench
                : Object.hasOwn(roleNames, node.id) || modelStages.has(node.id) ? Bot : GitBranch;
              const last = node.steps.at(-1);
              return <button key={node.id} type="button" data-node={node.id} className="trace-node"
                style={{ top: inset + node.index * rowHeight, height: nodeHeight }}
                aria-pressed={selected.id === node.id} aria-controls="trace-node-history"
                onClick={() => setSelection(node.id)}>
                <Icon size={17} aria-hidden="true" />
                <span><strong>{name(node.id)}</strong>
                  <small>{last ? label(last.status) : 'Ожидает вызова'}{node.steps.length > 1 && ` · ×${node.steps.length}`}</small>
                </span>
                <ChevronRight size={16} aria-hidden="true" />
              </button>;
            })}
          </div>
        </div>
        {edges.length > 0 && <ul className="trace-graph-legend">
          {Object.entries(edgeNames).filter(([kind]) => edges.some(edge => edge.kind === kind)).map(([kind, text]) =>
            <li key={kind}><span className={kind} aria-hidden="true" />{text}</li>)}
        </ul>}
      </div>
      <section id="trace-node-history" className="trace-graph-history" aria-labelledby="trace-node-title">
        <div className="trace-graph-history-heading">
          <h3 id="trace-node-title" aria-live="polite">{name(selected.id)}</h3>
          {selected.steps.length > 0 && <Badge value={selected.steps.at(-1).status} />}
        </div>
        {connections.length > 0 && <ul className="trace-graph-connections" aria-label="Связи выбранного узла">
          {connections.map(({ from, to, kind }) => <li key={from + '/' + to}>
            <span className="muted">{edgeNames[kind]}</span>
            <span>{name(from)} → {name(to)}</span>
          </li>)}
        </ul>}
        <h4>{tools.has(selected.id) || Object.hasOwn(roleNames, selected.id) ? 'История вызовов' : 'История обработки'}</h4>
        {selected.steps.length ? <ul key={selected.id} className="timeline">
          {selected.steps.map((step, index) => <Step key={step.id} title={title(step)} status={step.status}
            at={step.at ? time(step.at) : step.saved_at ? 'Сохранено ' + time(step.saved_at) : 'Время не сохранено'}
            initiallyOpen={index === 0 && selected.id !== 'publication'}>{renderEvidence(step)}</Step>)}
        </ul> : <Empty>Передача задачи записана. Вызов пока не сохранён.</Empty>}
      </section>
    </div>
  </Panel>;
}
