"""Opt-in acceptance of the routing model on a fixed, manually labeled Russian sample."""

from collections import Counter
from pathlib import Path
import sys

from news_futures.agents.routing import route_publication
from news_futures.common.artifacts import write_json


COMPANIES = {
    'SBER': 'Сбербанк', 'GAZP': 'Газпром', 'LKOH': 'Лукойл',
    'ROSN': 'Роснефть', 'NVTK': 'Новатэк', 'AFLT': 'Аэрофлот',
}
CASES = {
    'single': ('Сбербанк опубликовал квартальный отчёт: чистая прибыль выросла на 12%.',
               {'SBER': 'direct'}),
    'multiple': ('Газпром и Новатэк сообщили о приостановке отгрузок СПГ из-за аварии в порту.',
                 {'GAZP': 'direct', 'NVTK': 'direct'}),
    'none': ('В городском парке открыли бесплатную выставку детских рисунков.', {}),
    'industry': ('Из-за закрытия воздушного пространства российским пассажирским авиакомпаниям '
                 'придётся отменить часть рейсов.', {'AFLT': 'inferred'}),
    'commodity': ('Нефть Brent подешевела на 20% за неделю. Добытчики, продающие нефть, '
                  'могут потерять часть выручки.', {'LKOH': 'inferred', 'ROSN': 'inferred'}),
    'regulation': ('Минфин предложил повысить налог на добычу нефти в России. '
                   'Проект касается нефтедобывающих компаний.', {'LKOH': 'inferred', 'ROSN': 'inferred'}),
    'uncertain': ('Одна из двух компаний, Газпром или Новатэк, возможно, переносит запуск '
                  'проекта СПГ. Компания пока не названа.', {'GAZP': 'uncertain', 'NVTK': 'uncertain'}),
}


def evaluate(directory: Path, config: Path) -> int:
    directory.mkdir(parents=True, exist_ok=False)
    catalog = directory / 'diagnostics.json'
    write_json(catalog, {'source': 'control', 'operations': {'catalog': {
        'availability': 'available', 'data': [
            {'company_id': f'eval-{ticker}', 'company_name': name, 'ticker': ticker}
            for ticker, name in COMPANIES.items()
        ],
    }}})
    cases = {}
    totals: Counter[str] = Counter()
    for name, (text, labels) in CASES.items():
        publication = directory / f'{name}.json'
        write_json(publication, {'id': f'eval:{name}', 'url': f'https://example.invalid/{name}', 'text': text})
        trace = route_publication(config, publication, catalog, directory / name)
        expected = {f'eval-{ticker}': kind for ticker, kind in labels.items()}
        actual = ({link['company_id']: link['kind'] for link in trace['links']}
                  if trace['status'] == 'completed' else None)
        cases[name] = {
            'expected': expected, 'actual': actual, 'error': trace['error'],
            'model': trace['actual_model'], 'cost_usd': trace['cost_usd'],
        }
        if actual is None:
            totals['failed'] += 1
            continue
        totals['exact_cases'] += int(actual == expected)
        totals['matched_links'] += len(expected.keys() & actual.keys())
        totals['expected_links'] += len(expected)
        totals['actual_links'] += len(actual)
        totals['correct_kinds'] += len(expected.items() & actual.items())
    write_json(directory / 'evaluation.json', {'cases': cases, 'totals': dict(totals)})
    print(directory / 'evaluation.json')
    return int(totals['failed'] > 0)


if __name__ == '__main__':
    raise SystemExit(evaluate(Path(sys.argv[1]), Path('news_futures/agents/jev-model.json')))
