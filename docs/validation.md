# Validation: does the peak-risk rule catch real peaks?

Reproduce everything with `make backtest` (offline, about 3 seconds). The committed result is [backtest/results.json](../backtest/results.json); a test fails if code changes silently alter it.

## What is being tested

Ontario's ICI charges large customers by their share of demand in the **five highest-demand days** of the base period (1 May to 30 April), counting each day's highest hour. The rule flags hours at risk of being one of those five. A flagged hour is an *alert-hour*: a customer would be asked to curtail during it. The rule has two numbers that must always be read together: **peaks caught** (out of 5 per base period) and **alert-hours** (the cost of catching them).

## Method

**Ground truth.** IESO's own Peak Tracker for 2022, 2023 and 2025. The 2024 tracker file actually holds May 2025, so 2024's five peaks are **derived** from `ICIDemand_2024`. The derivation (highest hour of each day, then the top five days) reproduces IESO's tracker exactly for 2022, 2023 and 2025, which is the evidence it is a fair stand-in. 2021 is excluded (the file is empty).

**Only what was known at the time.** Every hour is decided using demand that had been public at the decision time. An hour's demand is treated as public **15 minutes after it ends**. That comes from this project's own data: of 137 normal hours, 135 were published within 20 minutes (median 11.7). Two arrived up to about 2 hours late. A further 107 hours were excluded from that measurement because they only appear late in the archive during the archiver's pause, which says nothing about IESO. A day's peak counts toward the bar only once that day is over.

**The rule.** The bar is the **5th-highest daily peak so far in the base period**. An hour is flagged if the estimated demand reaches `margin x bar`, and the hour falls inside a plausible peak window. Before five complete days exist, nothing is flagged.

**Four ways to estimate an hour's demand:**

| Variant | Estimate | Notice before the hour |
|---|---|---|
| `oracle` | The hour's own demand. Not usable live: a ceiling showing how selective the rule is. | none |
| `persist2` | The latest published hour (2 hours old) | about 45 minutes |
| `persist4` | The latest hour that was public 2 hours earlier | about 2 hours 45 minutes |
| `day_ahead` | Yesterday's peak, known as soon as the day ends | about 16 hours |

Forecast-based variants cannot be tested: no historical forecasts exist for 2022 to 2025 (IESO deletes them), and the forecast archive started on 28 Aug 2026, after this year's peaks.

**Window and margin: chosen from 2022 and 2023 only.** The window is the months and hour-endings seen in the training peaks (June to September, hours 12 to 18), widened by one hour on each side, giving **June to September, hour-ending 11 to 19**. The margin is the largest value on a 0.80 to 1.00 grid that still catches at least 8 of the 10 training peaks (so the fewest alert-hours). This rule was fixed before any result was seen.

**Held-out scoring:** 2024 and 2025 were scored once with those frozen choices. A test proves tuning never reads them.

## Results

Held-out base periods 2024 and 2025: 10 peaks (5 derived, 5 IESO-confirmed).

| Variant | Margin | Peaks caught | Alert-hours (2 years) | Caught by chance, same hours | Median notice |
|---|---|---|---|---|---|
| `oracle` (ceiling) | 1.00 | **10 / 10** | 241 | 1.1 | 0 |
| `persist2` | 0.98 | **10 / 10** | 245 | 1.1 | 0.8 h |
| `persist4` | 0.99 | **7 / 10** | 122 | 0.6 | 2.8 h |
| `day_ahead` | 1.00 | **8 / 10** | 441 | 2.0 | 16.5 h |

"Caught by chance" is how many peaks a rule would catch by flagging the same number of hours at random inside the window.

Training years (the 8-of-10 target was met, and tuning was allowed to look at these): `persist2` caught 8 of 10 (2022: 3 of 5), `persist4` 8 of 10, `day_ahead` 8 of 10. Across all four years: `persist2` 18 of 20, `day_ahead` 16 of 20, `persist4` 15 of 20, `oracle` 20 of 20.

Misses on held-out years: `persist4` missed 2024-08-27, 2025-07-24 and 2025-07-28; `day_ahead` missed 2024-08-27 and 2025-07-24.

## How to read it

- **A real tradeoff between notice and cost.** With 45 minutes of notice the rule caught all 10 held-out peaks using about 122 alert-hours a year (1.4% of the year's hours). Pushing notice to 2 hours 45 minutes halves the alert-hours but loses 3 peaks. Day-ahead notice (16 hours) is the most useful in practice, but caught 8 of 10 and cost about 220 alert-hours a year.
- **All variants beat chance by a wide margin** (for example 10 caught against 1.1 expected). The signal is real, not noise.
- **Even a perfect forecast costs about 120 alert-hours a year** (the oracle). Most of that is early-season uncertainty: until the year's top five are known, any warm day could still become one. The practical ceiling is not zero.
- **45 minutes of notice may be too short to be useful.** Whether a factory can curtail that quickly is a business question this backtest cannot answer.

**Honest result sentence:** On held-out base periods 2024 and 2025, the persistence rule with 45 minutes of notice flagged 10 of 10 peak hours (5 IESO-confirmed, 5 derived) while issuing 245 alert-hours, where flagging the same number of hours at random would catch about 1. With about 16 hours of notice, the day-ahead rule caught 8 of 10 with 441 alert-hours. Sample size is 10; see limitations.

## Limitations

- **Tiny sample.** 10 held-out peaks, and they cluster on consecutive hot days (for example 23 and 24 June 2025), so the number of independent events is smaller than 10. One more or fewer catch changes a headline by 10 points. Do not read `persist2`'s 10 of 10 as a rate.
- **The held-out years are not fully blind.** The author had seen the 2025 peak hours earlier in the project (Weekend 0). The one-hour window widening was fixed before the held-out run, but it is the reason the 2025 peaks at hour-ending 19 fall inside the window.
- **Training results were weaker than held-out.** `persist2` caught 8 of 10 in training and 10 of 10 held-out. That gap is within what 10 events can produce by luck.
- **No weather, no forecasts.** The estimates are crude. Forecast-based notice could not be measured at all.
- **2024's peaks are derived, not IESO-confirmed,** and `ICIDemand_2024` has missing hours (none near the peaks). 2025 is missing 2 hours; one is on a peak day (2025-07-24 hour-ending 19), but IESO's tracker confirms that day's peak was hour-ending 17. The current 2026 base period is not final until April 2027 and is not used.
- **Alert-hours are a count, not a cost.** There is no model of what each alert-hour costs a customer.
- **A late-published hour** (2 of 137 normal hours, up to about 2 hours) would only have given the live system *less* information than the backtest assumed.
- **Weekends are not excluded on purpose:** a Sunday peak (2022-08-07) exists.

## What guards this result

- A property test: rewriting any data published after a decision never changes that decision, and a companion test shows the property would catch a rule that peeks.
- A test that changing the 2024 and 2025 data leaves the window and every margin unchanged.
- A test that the committed `backtest/results.json` equals a fresh run.
- A test that the derived top five equals IESO's tracker for 2022, 2023 and 2025.
