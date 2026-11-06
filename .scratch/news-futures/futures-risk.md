# Денежный риск фьючерса: единицы и границы

Проверено 2026-11-06 по официальным источникам. Q33: аналитик предлагает стоп, код считает объём. Первый контур — sandbox, фьючерс на отдельную акцию, одна позиция. Торговых вызовов и выбора численных лимитов нет.

## Цена и объём

Котировка фьючерса — в пунктах. Деньги: `V(P) = P / min_price_increment × min_price_increment_amount`. Вывод: ценовой риск контракта до стопа `R = abs(entry − stop) / min_price_increment × min_price_increment_amount` при одинаковых единицах entry/stop. Стоимость шага меняется: обновлять через `GetFuturesMargin`. [Формула](https://developer.tbank.ru/invest/intro/useful-info/points), [Свежесть шага](https://developer.tbank.ru/invest/services/instruments/faq_instruments).

`PostOrder.quantity` — **лоты**. `Future.lot` — лотность фьючерса; `basic_asset_size` — размер базового актива. Последний нельзя повторно умножать на денежную формулу: пересчёт уже задан стоимостью шага. Для первого инструмента подтвердить `lot=1` и сверить расчёт с биржевой спецификацией: тогда лоты заявки равны контрактам. Иную лотность не поддерживать без проверки пересчёта. [quantity](https://developer.tbank.ru/invest/services/orders/faq_orders), [Future](https://developer.tbank.ru/invest/services/instruments/methods).

## Три разные проверки

Проектный вывод: целое число лотов должно удовлетворять одновременно:

- бюджету риска до стопа с учётом издержек;
- пределу номинальной экспозиции `N × V(entry)` относительно стоимости счёта;
- требованиям брокера к обеспечению и доступному объёму.

ГО отличается от номинала и максимального убытка. `GetFuturesMargin` даёт `initial_margin_on_buy`/`initial_margin_on_sell`; `GetMarginAttributes` — ликвидный портфель, маржу, заблокированное ГО. `Свободные деньги / ГО` не заменяет проверку брокера. [ГО](https://developer.tbank.ru/invest/services/instruments/methods), [Счёт](https://developer.tbank.ru/invest/services/accounts/users).

Произвольный параметр «плечо ×» не нужен: отношение номинала к стоимости счёта получается из объёма. Допуск брокера не задаёт пользовательский лимит риска. API имеет `GetMaxLots`/`GetOrderPrice`; поведение для выбранного фьючерса в sandbox ещё проверить. [Методы](https://developer.tbank.ru/invest/services/orders/methods).

## Что нельзя обещать

R — плановый риск, **не гарантированный максимум**: гэп, проскальзывание, комиссии, неисполнение лимитной заявки меняют результат. Проектный вывод: отклонять нулевое расстояние, стоп с неверной стороны, цену вне шага, неизвестные/устаревшие параметры. [Исполнение](https://developer.tbank.ru/invest/services/orders/faq_orders), [Шаг цены](https://developer.tbank.ru/invest/services/quotes/faq_marketdata).

`expire_date`/`GoodTillDate` — окончание **действия стопа**, не закрытие позиции. Native таймер выхода этим не подтверждён. [Описание](https://developer.tbank.ru/invest/services/stop-orders/head-stoporders), [EXPIRED](https://developer.tbank.ru/invest/services/stop-orders/stoporders).

Дневной убыток включает переоценку открытой позиции. Вариационную маржу нельзя прибавлять повторно к уже учитывающему её изменению стоимости счёта. Поля daily_yield/var_margin есть; схема сверки и границы дня здесь не установлены. [Портфель](https://developer.tbank.ru/invest/services/operations/methods).

## Данные и неподтверждённые возможности

Нужны UID/спецификация, lot/базовый актив, валюта, свежие шаг/стоимость шага/ГО/цены, стоп, стоимость счёта/свободные средства/маржа, позиции/заявки, комиссии/исполнения. Валюта расчётов согласована.

MCP заявляет котировки, портфель, заявки, стопы и sandbox, но доступность всех полей/методов не доказана. Проверить `tools/list` и схемы; при пробеле — полный API. Равенство интерфейсов не предполагается. [MCP](https://developer.tbank.ru/invest/mcp). Подробности защиты: [platform-protection.md](platform-protection.md).
