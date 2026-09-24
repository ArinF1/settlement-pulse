# Case study: making settlement exceptions visible

## Business question

If an operations team receives instruction and event feeds, which instructions are still unsettled after their intended date, and can a stakeholder trust the resulting count?

## Approach

I defined a small input contract, checked each row before loading it, and put invalid records in an explicit rejection export. The pipeline builds an as-of snapshot from event history, then presents an on-time rate, open exceptions, market breakdown, and exposure by original currency. The same source files produce the same report on every run.

## What the synthetic example shows

At 2026-06-30, the generated sample has **240 valid instructions**, **179 settled on time (74.6%)**, **33 open exceptions**, and **3 rejected source rows**. The 33 are the operational follow-up list. The three rejections deserve a separate data-quality investigation; hiding them would make the report look cleaner than the input really is. The market figures are synthetic and are not evidence of any real market or institution's performance.

## Data engineering decisions

- The snapshot uses only events known by the reporting cutoff; a later settlement does not rewrite a historical report.
- A resolved failure remains in event history but drops out of the current open-exception queue.
- Counts can be compared across markets. Monetary exposure stays split by SEK, EUR, DKK, and NOK because adding currencies would be misleading without an FX policy.
- SQLite is enough to make SQL transformations and output auditable without a hosted database or credentials.

## Next step in a real team

I would validate definitions and source ownership with operations and data governance colleagues, then add agreed market calendars and partial-settlement handling before using these metrics in a production process. This demo does not calculate regulatory penalties or submit regulatory reports.
